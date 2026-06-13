# Reconciliation Telemetry Namespace — Implementation Plan

## 1. Rationale

The reconciliation kernel (reconciliation-kernel/) has a complete operational pipeline:
FSM → engine → substrate → worker. It emits observability transitions with
`domain: 'reconciliation'` at every FSM state change (IDLE, RECONCILING,
CONVERGENT, DRIFTED), carrying `raw.epochCount`, `raw.driftCounters`,
`raw.escalationSignaled`.

But NO projection worker reads these emissions. The reconciliation system's
health is invisible to the telemetry plane — same gap that existed for
persist-telemetry before the previous fix.

Adding a reconciliation namespace (7th projection namespace) allows:
- Periodic synthesis of reconciliation health (state, drift rate, epoch counts)
- FSM-coordinated SEMANTIC_PROJECTION_TRANSITION entries in the canonical ledger
- Interpreter-accumulated projection state for CK observability queries
- Retry workers for staging buffer replay on failure

## 2. Signal Sources

Two emission sites produce transitions in the `reconciliation` domain:

**Source 1 — reconciliation-kernel/fsm.js (line 396-409)**
```
obs.transition({
  domain: 'reconciliation',
  entity: 'fsm',
  entityId: 'reconciliation-fsm',
  previousState: from,     // e.g. IDLE, RECONCILING, CONVERGENT, DRIFTED
  nextState: target,       // resolved target after transition
  authority: 'reconciliation-fsm',
  raw: {
    intent: event.type,           // RECONCILIATION_TICK, RESULTS_RECEIVED, CYCLE_COMPLETE
    epochCount: _epochCount,
    driftCounters: { ..._driftCounters },
    escalationSignaled: _escalationSignaled,
  },
})
```

**Source 2 — reconciliation-kernel/fsm.js (line 123 — substrate state transition)**
```
obs.transition({
  domain: 'reconciliation',    // same domain
  entity: 'substrate',
  ...
})
```

Both carry the same domain — the input module reads all domain='reconciliation'
transitions.

## 3. Files to Create (6 NEW files)

### 3.1 `telemetry-kernel/substrates/projection/inputs/reconciliation-input.js`

Reads last N transitions for `domain='reconciliation'` from the observability
transition log. Returns window of signals for the synthesis stage.

Contract:
```javascript
async function getNormalizedInputWindow({ pollIntervalMs, tickCount, windowSize } = {})
  → {
      transitions: Array,       // raw transitions from observability
      now: number,              // wall clock at read time
      tickCount: number,
      windowOpenedAt: number,
      windowClosedAt: number,
      entryCount: number,
      noiseGate: boolean,       // true if < 3 entries
      source: 'observability.transitionLog[reconciliation]',
    }
```

DEFAULT_WINDOW_SIZE = 50
DEFAULT_POLL_INTERVAL_MS = 30_000

Reads ALL transitions for domain='reconciliation' (both entity='fsm' and
entity='substrate'). Uses `observability.query.getTransitionLog('reconciliation', null, windowSize)`.

### 3.2 `telemetry-kernel/substrates/projection/synthesis/reconciliation-projection.js`

Pure function from signals → semantic payload. No I/O, no external state.

```javascript
function synthesize(_projectionState, signals)
  → {
      currentReconciliationState: string,   // last nextState from FSM transitions
      epochCount: number,                   // total cycles from raw.epochCount
      driftedEpochCount: number,            // driftCounters.substrate from last transition
      replayDriftCount: number,             // driftCounters.replay
      escalationSignaled: boolean,          // raw.escalationSignaled
      consecutiveDrifted: number,           // from FSM internal state (derived from transitions)
      consecutiveConverged: number,
      driftRate: number,                    // driftedEpochCount / epochCount (if epochCount>0)
      totalTransitions: number,
      isStale: boolean,                     // aging > 5 min
    }

function computeConfidence(signals)     // same noise-gate pattern as other projections
function computeIntegrityScore(signals) // same pattern
```

Helper functions:
- `deriveCurrentState(transitions)` — walks backwards, finds last FSM transition, returns its `nextState`
- `computeDriftRate(transitions)` — extracts last raw.driftCounters, computes rate
- `computeAging(transitions, now)` — ms since last transition

### 3.3 `telemetry-kernel/substrates/projection/workers/reconciliation-projection-worker.js`

Extends BaseProjectionWorker:

```javascript
class ReconciliationProjectionWorker extends BaseProjectionWorker {
  constructor()
    super({ pollIntervalMs: 30_000, workerName: 'reconciliation-projection-worker' })

  get _projectType()    → 'RECONCILIATION_PROJECTION'
  get _domain()         → 'reconciliation'

  async _getNormalizedInputWindow()   → reconciliationInput.getNormalizedInputWindow(...)
  _runSynthesis(state, signals)       → reconciliationSynthesis.synthesize(state, signals)
  _computeConfidence(signals)         → reconciliationSynthesis.computeConfidence(signals)
  _computeIntegrityScore(signals)     → reconciliationSynthesis.computeIntegrityScore(signals)
}
```

### 3.4 `telemetry-kernel/substrates/projection/transition-writers/reconciliation-transition-writer.js`

4-line file following the exact pattern of every other transition writer:

```javascript
const { createTransitionWriter } = require('./base-transition-writer');
const writer = createTransitionWriter('reconciliation');
module.exports = { start: writer.start, stop: writer.stop, getHealth: writer.getHealth, awaitPendingWrite: writer.awaitPendingWrite };
```

### 3.5 `retry-cadence-kernel/workers/telemetry-retry-reconciliation-worker.js`

Drains `lineage:projection-staging:reconciliation` Redis list, re-emits each
entry through `observability.transition()`. Mirrors
`telemetry-retry-persist-telemetry-worker.js` exactly with NAMESPACE = 'reconciliation'.

Key values:
- NAMESPACE = 'reconciliation'
- STAGING_KEY = 'lineage:projection-staging:reconciliation'
- domain in re-emit = 'reconciliation'
- entity = 'semantic_projection'
- raw.entryType = 'SEMANTIC_PROJECTION_TRANSITION'
- raw.projectionNamespace = 'reconciliation'
- authority = 'reconciliation-projection-worker'

## 4. Files to Modify (7 EXISTING files)

### 4.1 `telemetry-kernel/substrates/projection/workers/index.js`

Add to imports:
```javascript
const ReconciliationProjectionWorker = require('./reconciliation-projection-worker');
```

Add to workers map:
```javascript
workers: {
  // ...existing 7 workers...
  reconciliation: new ReconciliationProjectionWorker(),
}
```

Update startAll order to include 'reconciliation' (position 4 — before capability):
```javascript
const order = ['systemic', 'health', 'integrity', 'authority', 'runtime', 'reconciliation', 'capability', 'persistTelemetry'];
```

Update stopAll order:
```javascript
const order = ['persistTelemetry', 'capability', 'reconciliation', 'runtime', 'authority', 'integrity', 'health', 'systemic'];
```

Update console.log: 'All 7' → 'All 8'

### 4.2 `telemetry-kernel/substrates/projection/transition-writers/base-transition-writer.js` (line 53)

```javascript
// Before:
const NAMESPACES = ['runtime', 'integrity', 'authority', 'health', 'systemic', 'persist-telemetry'];
// After:
const NAMESPACES = ['runtime', 'integrity', 'authority', 'health', 'systemic', 'persist-telemetry', 'reconciliation'];
```

### 4.3 `telemetry-kernel/substrates/projection/transition-writers/index.js`

Add to imports:
```javascript
const reconciliationWriter = require('./reconciliation-transition-writer');
```

Add to writers map:
```javascript
writers: {
  // ...existing...
  'reconciliation': reconciliationWriter,
}
```

Update console.log: 'All 7' → 'All 8'

### 4.4 `telemetry-kernel/fsm.js`

Three locations:

**NAMESPACE_ORDER_PRIORITY (line 108-116):**
```javascript
const NAMESPACE_ORDER_PRIORITY = {
  integrity: 1, authority: 2, runtime: 3, health: 4, systemic: 5,
  capability: 6, 'persist-telemetry': 7, 'reconciliation': 8,
};
```

**KNOWN_PROJECTION_NAMESPACES (line 118-120):**
```javascript
const KNOWN_PROJECTION_NAMESPACES = new Set([
  'integrity', 'authority', 'runtime', 'health', 'systemic',
  'capability', 'persist-telemetry', 'reconciliation',
]);
```

**INTENT_NAMESPACES (line 166):**
```javascript
const INTENT_NAMESPACES = Object.freeze([
  'runtime', 'integrity', 'authority', 'health', 'systemic',
  'persist-telemetry', 'reconciliation',
]);
```

Note: INTENT_NAMESPACES controls which namespaces the FSM reads and validates
intents from during coordination cycles. Adding 'reconciliation' ensures
reconciliation PROJECTION_INTENT entries are processed.

### 4.5 `acquisition-kernel/substrate-registry.js`

**RETRY_WORKER_MAP (after line 78):**
```javascript
'telemetry:reconciliation': '../retry-cadence-kernel/workers/telemetry-retry-reconciliation-worker',
```

**CLASSIFICATION_WORKER_MAP (after line 117):**
```javascript
'telemetry:reconciliation': '../retry-cadence-kernel/workers/classification-worker',
```

**DOMAIN_REGISTRY (after line 182):**
```javascript
'telemetry:reconciliation': { },
```

### 4.6 `retry-cadence-kernel/policy.js`

**POLICIES (after line 85):**
```javascript
'telemetry:reconciliation': { maxRetries: 3, baseDelayMs: 15000, maxDelayMs: 60000, backoffMultiplier: 1.5 },
```

**DOMAIN_TO_SUBSTRATE (after line 129):**
```javascript
'telemetry:reconciliation': 'telemetry:reconciliation',
```

### 4.7 `control-plane/governance/interpreters/namespace-projection-interpreter.js`

**Default projection slot (line 36):**
```javascript
'reconciliation': { state: 'IDLE', transitionCount: 0, lastTransition: null, epochCount: 0, driftedEpochCount: 0, escalationSignaled: false, driftRate: 0 },
```

**Case in switch (after 'persist-telemetry' case):**
```javascript
case 'reconciliation':
  if (payload.currentReconciliationState) {
    _projections.domain.reconciliation.state = payload.currentReconciliationState;
  }
  if (payload.epochCount !== undefined) {
    _projections.domain.reconciliation.epochCount = payload.epochCount;
  }
  if (payload.driftedEpochCount !== undefined) {
    _projections.domain.reconciliation.driftedEpochCount = payload.driftedEpochCount;
  }
  if (payload.escalationSignaled !== undefined) {
    _projections.domain.reconciliation.escalationSignaled = payload.escalationSignaled;
  }
  if (payload.driftRate !== undefined) {
    _projections.domain.reconciliation.driftRate = payload.driftRate;
  }
  if (payload.projectionId || payload.timestamp) {
    _projections.domain.reconciliation.transitionCount++;
    _projections.domain.reconciliation.lastTransition = payload.timestamp || Date.now();
  }
  break;
```

## 5. Execution Order

```
Phase A — Create 6 new files (one-shot)
  A1. reconciliation-input.js
  A2. reconciliation-projection.js (synthesis)
  A3. reconciliation-projection-worker.js
  A4. reconciliation-transition-writer.js
  A5. telemetry-retry-reconciliation-worker.js

Phase B — Modify 7 existing files (one-shot)
  B1. telemetry-kernel/substrates/projection/workers/index.js
  B2. base-transition-writer.js (NAMESPACES)
  B3. transition-writers/index.js
  B4. telemetry-kernel/fsm.js (3 locations)
  B5. substrate-registry.js (3 locations)
  B6. retry-cadence/policy.js (2 locations)
  B7. namespace-projection-interpreter.js (default slot + switch case)

Phase C — Verification
  C1. node --check on all 13 files
  C2. Report results
```

## 6. Verification

Each file must pass `node --check`. No boot test (requires running server).
The projection worker will emit PROJECTION_INTENT on its first tick (30s after
start). The FSM coordination cycle will pick it up on the next tick. The
transition writer will receive it if domain matches and entryType is correct.

## 7. Risk

Low — exact mirror of persist-telemetry pattern. The 6 existing projection
workers all follow the same structure. 7th and 8th follow identically.

## 8. Files Not Touched

- `control-plane/governance/constitutional-kernel.js` — DOMAIN_EVENT_MAP already has RECONCILIATION_* entries. No routing changes needed.
- `reconciliation-kernel/` — no changes to the pipeline itself. Only adding telemetry visibility of its emissions.
- Any test files — will update in a later pass.
