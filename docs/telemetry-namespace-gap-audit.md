# Telemetry Namespace Gap Audit — 2026-06-12

## Architecture: The 4-Layer Stack (per namespace)

Every domain that needs telemetry visibility requires a 4-layer stack:

```
Layer 1 — INPUT + PROJECTION WORKER
  Reads domain's transitions from observability, synthesizes semantic payload,
  emits PROJECTION_INTENT with `domain: <namespace>`.

Layer 2 — FSM COORDINATION
  Telemetry-coordination FSM reads PROJECTION_INTENT entries, validates,
  orders, serializes, re-emits as SEMANTIC_PROJECTION_TRANSITION with
  `raw.entryType` + `coordinatedBy` + `domain: <namespace>`.

Layer 3 — TRANSITION WRITER
  onWrite subscriber. Gates: entryType === 'SEMANTIC_PROJECTION_TRANSITION'
  AND domain === <namespace>. Writes to canonical lineage ledger.
  Dispatches PROJECTION_PERSISTED to CK.

Layer 4 — INTERPRETER CASE
  namespace-projection-interpreter.  _computeDomainProjection(domain, entry)
  switch(domain) case '<namespace>': accumulates projection state.
```

## Existing 6 Namespaces (COVERED ✓)

| Namespace | Projection Worker | Input | Synthesis | Transition Writer | Retry Worker | Interpreter Case | Substrate Registry | Policy |
|---|---|---|---|---|---|---|---|---|
| runtime | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 108) | ✓ | ✓ |
| integrity | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 80) | ✓ | ✓ |
| authority | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 98) | ✓ | ✓ |
| health | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 117) | ✓ | ✓ |
| systemic-pressure | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 127) | ✓ | ✓ |
| capability | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (line 139) | ✓ | ✓ |

## 13+ Domains Emitting Uncovered Emissions (MISSING)

These domains emit transitions to observability but have NO projection worker reading them:

| Domain | Emitted By | Obs Emission Shape | Risk |
|---|---|---|---|
| `acquisition` | acquisition-kernel/fsm.js | domain:'acquisition' entity:'intent-span'/'fsm' | **HIGH** — intent lifecycle invisible |
| `content` | publishing-kernel substrates | domain:'content' | MEDIUM — covered by publishing |
| `dedup` | dedup-kernel/fsm.js | domain:'dedup' | **HIGH** — silent dedup failures |
| `dedup:redis` | dedup-kernel/fsm.js | domain:'dedup:redis' | LOW — subdomain |
| `dedup:mutation` | dedup-kernel/fsm.js | domain:'dedup:mutation' | LOW — subdomain |
| `dedup:emission` | dedup-kernel/fsm.js | domain:'dedup:emission' | LOW — subdomain |
| `engagement` | publishing-kernel substrates | domain:'engagement' | MEDIUM |
| `insights` | acquisition-kernel transport | domain:'insights' | MEDIUM |
| `persist-telemetry` | postgres-telemetry-kernel/fsm.js | domain:'persist-telemetry' | **HIGH** — DB write failures invisible |
| `publish:post` | publishing-kernel/fsm.js | domain:'publish:post' | MEDIUM — covered by publishing |
| `publishing` | publishing-kernel/fsm.js | domain:'publishing' | **HIGH** — pipeline health invisible |
| `reconciliation` | reconciliation-kernel/fsm.js | domain:'reconciliation' | **HIGH** — drift detection invisible |
| `scheduling` | scheduling-kernel/fsm.js | domain:'scheduling' | **HIGH** — cadence failures invisible |
| `telemetry` | telemetry-kernel/fsm.js | domain:'telemetry' | LOW — self-referential |

## Detailed Gap Per File

### File 1: telemetry-kernel/fsm.js (line 118-119, 166)
```javascript
KNOWN_PROJECTION_NAMESPACES = new Set(['integrity','authority','runtime','health','systemic','capability']);
INTENT_NAMESPACES = Object.freeze(['runtime','integrity','authority','health','systemic']);
```
**Gap:** Missing `persist-telemetry`, `acquisition`, `publishing`, `reconciliation`, `dedup`, `scheduling`. Both sets should include the new domains.

### File 2: telemetry-kernel/substrates/projection/workers/index.js
**Gap:** Workers for `acquisition`, `publishing`, `dedup`, `reconciliation`, `scheduling`, `persist-telemetry` do not exist. `acquisition-projection-worker.js`, `publishing-projection-worker.js`, etc. need creation.

### File 3: telemetry-kernel/substrates/projection/transition-writers/base-transition-writer.js (line 53)
```javascript
const NAMESPACES = ['runtime', 'integrity', 'authority', 'health', 'systemic'];
```
**Gap:** Missing `capability` (was added as a worker but not to this array!) and all new domains.

### File 4: telemetry-kernel/substrates/projection/transition-writers/index.js
**Gap:** Already auto-maps from `writers` object keys via `Object.freeze(Object.keys(writers))`. If the writer file exists, it's auto-picked. But the writer files for new domains don't exist yet.

### File 5: control-plane/governance/interpreters/namespace-projection-interpreter.js
**Gap:** `_projections.domain` at lines 30-35 has default slots for `acquisition`, `publishing`, `scheduling`, `dedup`, `reconciliation`, `capability`. But `_computeDomainProjection()` switch (lines 79-155) has NO cases for `acquisition`, `publishing`, `scheduling`, `dedup`, `reconciliation`. The runtime case (line 108-114) indirectly populates them via the runtime projection payload, but this is a fragile single-point-of-update pattern.

### File 6: telemetry-kernel/substrates/projection/inputs/
**Gap:** 13 missing input modules (`acquisition-input.js`, `publishing-input.js`, etc.) — one per uncovered domain. Each reads last N transitions for its domain from the observability transition log.

### File 7: telemetry-kernel/substrates/projection/synthesis/
**Gap:** 13 missing synthesis modules (`acquisition-projection.js`, `publishing-projection.js`, etc.) — pure function converting raw signals → semantic projection payload.

### File 8: retry-cadence-kernel/policy.js (lines 80-85, 122-129)
Already has 6 `telemetry:*` domains. New domains need `telemetry:<name>` entries.

### File 9: acquisition-kernel/substrate-registry.js (lines 71-76, 112-117, 176-181)
Already has 6 `telemetry:*` domains. New domains need entries.

### File 10: retry-cadence-kernel/workers/
Already has 6 `telemetry-retry-*-worker.js` files. 13 more needed.

### File 11: orchastrator.js
Needs `constitutional.registerWorker()` entries for new retry workers.

## Tests Requiring Update

| Test File | Assertion | Current Count |
|---|---|---|
| phase-6a-transition-writers-redis.test.js L148 | toBe(6) | 6 → must update to N |
| phase-0-architecture-validation.test.js L102 | "all 6 projection workers" | 6 → must update |
| phase-4k-telemetry-governance-coverage.test.js L151 | "all 5+ span emissions" | Accepts variable — ok |
| phase-4e-replay-reconstruction.test.js L113/119 | toBe(6) | 6 wave entries — ok |
| phase-6-telemetry-coordination-fsm.test.js L765 | "all 6 FSM gates" | 6 — ok |
| phase-4h-consumer-pressure.test.js L132 | toBe(5) | 5 — must update |
| phase-4n-mixed-constitutional-soak.test.js L388 | toBe(5) | 5 — must update |

## Current Weak Coverage Path

The interpreter's `_projections.domain.acquisition` etc. are populated ONLY through the `runtime` case:

```
runtime-projection-worker → runtime case (line 108-114)
  for (const [domainName, data] of Object.entries(payload)):
    if (_projections.domain[domainName]) → update
```

This means `acquisition` state depends on the RUNTIME projection worker including `acquisition` in its synthesis payload. If the runtime synthesis doesn't see enough domain transitions, the slot stays at default forever. There is NO direct feedback: if acquisition-kernel FSM transitions from IDLE to ACQUIRING, nobody detects it through telemetry.

## Risk Summary

The system has wide coverage (interpreter has default slots for 6 domain kernels) but ZERO direct projection workers for those kernels. All "domain kernel" telemetry depends on a single fragile update path through the `runtime` case. A dedicated projection worker per kernel domain (acquisition, publishing, dedup, etc.) is the correct fix.
