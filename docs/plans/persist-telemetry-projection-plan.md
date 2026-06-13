# Plan: Persist-Telemetry Projection Namespace

## Signal Sources

Two emission sources produce transitions in the `persist-telemetry` domain:

**Source 1 — postgres-telemetry-kernel/fsm.js (line 736-744)**
```
obs.transition({
  domain: 'persist-telemetry',
  entity: 'fsm',
  entityId: 'persist-telemetry-fsm',
  previousState: from,    // IDLE | WRITING | READING | ERROR_*
  nextState: target,
  raw: { intent: event.type, table: event.table || null, inFlight: _inFlight }
})
```
Event types: DB_WRITE_REQUESTED, DB_WRITE_COMPLETE, DB_READ_REQUESTED, DB_READ_COMPLETE, DB_PERSIST_FAILURE

**Source 2 — retry-cadence-kernel/fsm.js (lines 900-1002)**
```
// DB_PERSIST_FAILURE / DB_PERSIST_FAILURE_READ handlers
observability transition with domain: domain || 'persist-telemetry'
```
Carries failure analysis: category, severity, recommendations

## Files to Create (6 new files)

| # | File | Purpose | Mirrors |
|---|---|---|---|
| 1 | `telemetry-kernel/substrates/projection/inputs/persist-telemetry-input.js` | Reads last N transitions for domain='persist-telemetry' from observability. Returns window of transitions with entry count. | `capability-input.js` (reads graph-capability transitions) |
| 2 | `telemetry-kernel/substrates/projection/synthesis/persist-telemetry-projection.js` | Pure function: transitions → { writeCount, readCount, failureCount, failureRate, tableDistribution, currentState, inFlight, severityBreakdown } | `capability-projection.js` (synthesizes capability state) |
| 3 | `telemetry-kernel/substrates/projection/workers/persist-telemetry-projection-worker.js` | Extends BaseProjectionWorker, _domain='persist-telemetry', _projectType='PERSIST_TELEMETRY_PROJECTION', POLL_INTERVAL_MS=30000 | `runtime-projection-worker.js` |
| 4 | `telemetry-kernel/substrates/projection/transition-writers/persist-telemetry-transition-writer.js` | 4-line file: creates createTransitionWriter('persist-telemetry') | `runtime-transition-writer.js` |
| 5 | `retry-cadence-kernel/workers/telemetry-retry-persist-telemetry-worker.js` | Drains lineage:projection-staging:persist-telemetry, re-emits | `telemetry-retry-capability-worker.js` |

## Files to Modify (6 files)

| # | File | Change |
|---|---|---|
| 6 | `telemetry-kernel/substrates/projection/workers/index.js` | Add require + workers.persistTelemetry + startAll/stopAll entry |
| 7 | `telemetry-kernel/substrates/projection/transition-writers/base-transition-writer.js` (line 53) | Add 'persist-telemetry' to NAMESPACES array |
| 8 | `telemetry-kernel/substrates/projection/transition-writers/index.js` | Already auto-maps from writers object — add require |
| 9 | `telemetry-kernel/fsm.js` (lines 118-119, 166) | Add 'persist-telemetry' to KNOWN_PROJECTION_NAMESPACES and INTENT_NAMESPACES |
| 10 | `acquisition-kernel/substrate-registry.js` | Add 'telemetry:persist-telemetry' to RETRY_WORKER_MAP, CLASSIFICATION_WORKER_MAP, DOMAIN_REGISTRY |
| 11 | `retry-cadence-kernel/policy.js` | Add telemetry:persist-telemetry policy + DOMAIN_TO_SUBSTRATE entry |
| 12 | `control-plane/governance/interpreters/namespace-projection-interpreter.js` | Add case 'persist-telemetry': in _computeDomainProjection() switch |

## Existing Base That Already Works

- `base-projection-worker.js` — `_emitProjectionTransition()` emits PROJECTION_INTENT with `domain: this._domain` — worker just overrides `_projectType` and `_domain`
- `base-transition-writer.js` — filters on `domain === namespace` + `raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION'` — writer just passes namespace
- Telemetry-coordination FSM — reads PROJECTION_INTENT entries, validates, coordinates, re-emits SEMANTIC_PROJECTION_TRANSITION — no changes needed
- CK routing — PROJECTION_PERSISTED → FSM → interpreter — no changes needed

## What the Persist-Telemetry Projection Tracks

After a tick, the synthesis emits:

```javascript
payload: {
  writeCount: 47,             // DB writes in window
  readCount: 23,              // DB reads in window
  failureCount: 2,            // DB_PERSIST_FAILURE events
  failureRate: 0.029,         // failures / (writes + reads)
  currentState: 'WRITING',    // FSM state from last transition
  inFlight: 3,                // from raw.inFlight
  tableDistribution: {        // hit count per table
    instagram_credentials: 12,
    api_usage: 8,
    instagram_comments: 27,
  },
  severityBreakdown: {        // failure severities
    LOW: 0, MEDIUM: 1, HIGH: 1, CRITICAL: 0
  },
}
```

The interpreter case `persist-telemetry` accumulates: `writeCount`, `readCount`, `failureCount`, `currentState`, `lastTransition`.

## Test Updates

| Test | Line | Change |
|---|---|---|
| `phase-0-architecture-validation.test.js` | 102 | toBe(6) → toBe(7) |
| `phase-6a-transition-writers-redis.test.js` | 148, 199 | toBe(6) → toBe(7), add domain |
| `phase-4h-consumer-pressure.test.js` | 132 | toBe(5) → toBe(6) |
| `phase-4n-mixed-constitutional-soak.test.js` | 388 | toBe(5) -> toBe(6) |
