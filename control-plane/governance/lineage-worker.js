// control-plane/governance/lineage-worker.js
// Lineage Worker: canonical runtime interpretation and projection substrate.
//
// Owns: consuming observable transitions, producing immutable lineage entries,
//        synthesizing runtime projections, detecting continuity anomalies.
//
// Does NOT own: constitutional legitimacy, governance enforcement,
//               reconciliation verification, runtime mutation.
//
// Architectural identity:
//   worker = runtime interpretation substrate
//   reconciliation = truth verification
//   governance = constitutional legitimacy
//
// Internal separation (preserves replay determinism):
//   Layer A — Immutable Lineage Ingestion (deterministic, never interprets)
//   Layer B — Projection Interpretation Engine (evolvable, versioned)
//
// Consumption model:
//   Observability Plane → getEntriesSince(cursor) → Layer A → lineage:ledger:entries
//                                                              ↓
//                                                     Layer B → lineage:projection:snapshot
//
// Invariants:
//   - Worker never mutates runtime state
//   - Worker never enforces governance
//   - Worker never decides constitutional legitimacy
//   - Only normalized STATE_TRANSITION entries consumed
//   - Historical entries never rewritten
//   - All entries carry full causation graph

const crypto = require('crypto');
const lineageLedger = require('./lineage-ledger');

// ═══════════════════════════════════════════════════════════════════════════════
// Versioning — critical for replay determinism across projection evolution
// ═══════════════════════════════════════════════════════════════════════════════

const PROJECTION_VERSION = '1.0.0';
const LINEAGE_VERSION = '1.0.0';

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

const CONSUMER_NAME = 'lineage-worker';
const DEFAULT_POLL_MS = 5000;
const MIN_POLL_MS = 500;          // fastest: 500ms when under severe pressure
const MAX_POLL_MS = 5000;         // slowest: 5000ms when caught up
const SNAPSHOT_INTERVAL_MS = 15_000; // how often to persist projection snapshot
const HEALTH_TTL_S = 30; // worker health key TTL in Redis

// Adaptive poll thresholds
const PRESSURE_THRESHOLD_HIGH = 0.5;  // lag > 50% of MAX_LOG_ENTRIES → accelerate
const PRESSURE_THRESHOLD_LOW  = 0.2;  // lag < 20% of MAX_LOG_ENTRIES → decelerate
const MAX_LOG_ENTRIES = 10_000;

// Required fields for a valid transition — missing any → ingestion rejected
const REQUIRED_TRANSITION_FIELDS = ['traceId', 'correlationId', 'authority', 'timestamp', 'domain', 'entity'];

// ═══════════════════════════════════════════════════════════════════════════════
// Module state
// ═══════════════════════════════════════════════════════════════════════════════

let _cursor = 0; // current read position in observability _transitionLog
let _lastPersistedCursor = 0; // safely persisted position — only advances on confirmed persistence
let _consecutiveFailures = 0; // counter for backpressure detection (A4)
let _pollTimer = null;
let _snapshotTimer = null;
let _running = false;
let _startedAt = null;
let _lastTick = null;
let _tickCount = 0;
let _entryCount = 0;
let _currentPollMs = DEFAULT_POLL_MS; // adaptive poll interval — changes under pressure

// Deterministic commit visibility — callers await until _cursor >= their entryId
const _commitWaiters = new Map(); // entryId → { resolve, reject, timeout }

// In-memory lineage buffer — consumed by Layer B for projection synthesis
const _lineageBuffer = []; // Array<ledgerEntry>

// Layer B — projection state (evolvable, versioned)
//
// SIGNAL OWNERSHIP CONTRACT (CK governance):
//   Ledger-derivable signals only — recomputable from immutable lineage replay.
//   Observer-relative signals (failureRate, governancePressure, etc.) MUST NOT
//   be written here — they belong to telemetry-workers and are governed separately.
//
// See: SIGNAL_OWNERSHIP_MAP in constitutional-kernel.js
const _projections = {
  domain: {
    acquisition: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    publishing: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    scheduling: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0, cadenceContinuity: 1.0 },
    dedup: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    reconciliation: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
  },
  governanceRuntime: {
    runtimeState: 'BOOTING',
    degradationSignals: {},
    replayContinuity: 'intact',
    domainInstability: 0,
    epochCount: 0,
    lastStateTransition: null,
  },
  health: {
    executionHealth: 'STABLE',
    transitionCount: 0,
    lastTransition: null,
    authorityStability: 1.0,
  },
  authority: {
    acquisition: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    publishing: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    scheduling: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
  },
  integrity: {
    structuralAnomalyCount: 0,
    replayAnomalyProbability: 0,
    cadenceGapProbability: 0,
  },
};

// Divergence log — runtime continuity anomalies (NOT governance violations)
const _divergences = [];

// Track authorities per domain+entity for oscillation detection
const _authorityHistory = new Map(); // "domain:entity" → [{ authority, ts }]

// Track state transitions per entity for oscillation detection
const _stateHistory = new Map(); // "domain:entity:entityId" → [{ state, ts }]

// ═══════════════════════════════════════════════════════════════════════════════
// Layer A — Immutable Lineage Ingestion
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Validate a transition against the required contract.
 * Rejected transitions are logged as structural anomalies, never ingested.
 *
 * @param {object} transition — STATE_TRANSITION from observability plane
 * @returns {{ valid: boolean, reason?: string }}
 */
function _validateTransition(transition) {
  // traceId/correlationId are causal-chain fields — required only for FSM state transitions.
  // Projection workers emit SEMANTIC_PROJECTION_TRANSITION entries with worker-specific
  // domains (runtime, health, integrity, etc.) and do not participate in FSM causal chains.
  const isProjectionEntry =
    transition.domain === 'projection' ||
    (transition.raw && transition.raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION');

  const requiredFields = isProjectionEntry
    ? ['authority', 'timestamp', 'domain', 'entity']
    : REQUIRED_TRANSITION_FIELDS;

  for (const field of requiredFields) {
    if (transition[field] == null || transition[field] === '') {
      return { valid: false, reason: `missing required field: ${field}` };
    }
  }

  if (typeof transition.timestamp !== 'number' || isNaN(transition.timestamp)) {
    return { valid: false, reason: `invalid timestamp: ${transition.timestamp}` };
  }

  return { valid: true };
}

/**
 * Hash a value deterministically.
 *
 * @param {string|null} value
 * @returns {string} hex-encoded SHA-256
 */
function _hash(value) {
  return crypto.createHash('sha256').update(String(value ?? 'null')).digest('hex');
}

/**
 * Transform a validated STATE_TRANSITION into an immutable ledger entry.
 * This is pure — no side effects, no interpretation.
 *
 * @param {object} transition — validated STATE_TRANSITION
 * @returns {object} canonical ledger entry
 */
function _transformToEntry(transition) {
  return {
    ledgerId: crypto.randomUUID(),
    timestamp: transition.timestamp,
    ingestedAt: Date.now(),
    domain: transition.domain,
    entity: transition.entity,
    entityId: transition.entityId,
    previousState: transition.previousState,
    nextState: transition.nextState,
    authority: transition.authority,
    previousStateHash: _hash(transition.previousState),
    nextStateHash: _hash(transition.nextState),
    authorityHash: _hash(transition.authority),
    traceId: transition.traceId,
    correlationId: transition.correlationId,
    causationId: transition.causationId,
    parentTransitionId: transition.parentTransitionId,
    projectionVersion: PROJECTION_VERSION,
    lineageVersion: LINEAGE_VERSION,
    raw: transition, // full transition for deterministic replay
  };
}

/**
 * Layer A tick: consume from observability plane, validate, transform, persist.
 * This is deterministic — never interprets, only preserves.
 *
 * @returns {{ consumed: number, rejected: number, newCursor: number }}
 */
function _ingestTick() {
  const observability = require('../observability');

  const { entries, nextCursor } = observability.query.getEntriesSince(_cursor);
  if (entries.length === 0) return { consumed: 0, rejected: 0, newCursor: _cursor };

  let consumed = 0;
  let rejected = 0;
  let allPersisted = true;

  for (const transition of entries) {
    // Validate transition contract
    const validation = _validateTransition(transition);
    if (!validation.valid) {
      rejected++;
      _recordStructuralAnomaly('MALFORMED_TRANSITION', {
        reason: validation.reason,
        transitionTraceId: transition.traceId,
        transitionDomain: transition.domain,
      });
      continue;
    }

    // Transform to canonical ledger entry
    const entry = _transformToEntry(transition);

    // Detect structural anomaly: broken causation chain
    if (entry.parentTransitionId) {
      const parentExists = _lineageBuffer.some(e => e.traceId === entry.parentTransitionId);
      if (!parentExists) {
        _recordStructuralAnomaly('BROKEN_CAUSATION_CHAIN', {
          ledgerId: entry.ledgerId,
          parentTransitionId: entry.parentTransitionId,
          traceId: entry.traceId,
        });
      }
    }

    // Persist to ledger — track success for safe cursor advance (A3)
    const persisted = _persistEntry(entry);
    if (!persisted) allPersisted = false;

    // Buffer for Layer B
    _lineageBuffer.push(entry);
    consumed++;
    _entryCount++;
  }

  // Advance read cursor and feed back to projection (A2)
  _cursor = nextCursor;

  // Notify any callers waiting for commit visibility — cursor now at or past their entryId
  _notifyCommitWaiters();

  // Bounded-overflow detection: if logSize > cursor and log has hit MAX_LOG cap,
  // entries were silently evicted — this violates the constitutional invariant
  // that no source entries are silently lost. Halt ingestion until acknowledged.
  const logSize = observability.query.getLogSize();
  const MAX_LOG = 10_000;
  if (logSize > _cursor && logSize >= MAX_LOG) {
    // Entries have been silently evicted. Emit a divergence entry and halt.
    _recordStructuralAnomaly('LINEAGE_TRUNCATION_DETECTED', {
      readCursor: _cursor,
      logSize,
      maxLog: MAX_LOG,
      evictedCount: logSize - _cursor,
      consumedBeforeHalt: consumed,
    });
    // Do NOT update consumer cursor — this signals to governance that the
    // worker has stalled at the truncation boundary. Ingestion halts
    // by returning without advancing _lastPersistedCursor.
    _consecutiveFailures++;
    if (_consecutiveFailures >= 2) {
      _emitBackpressure();
    }
    return { consumed, rejected, newCursor: _cursor, truncated: true };
  }

  observability.query.updateConsumerCursor(CONSUMER_NAME, _cursor);

  // Safe cursor advance — only when all entries in tick persisted (A3)
  if (allPersisted) {
    _lastPersistedCursor = _cursor;
    _consecutiveFailures = 0;
    _persistCursor();
  } else {
    _consecutiveFailures++;
    // Backpressure signal on persistent failure (A4)
    if (_consecutiveFailures >= 3) {
      _emitBackpressure();
    }
  }

  return { consumed, rejected, newCursor: _cursor };
}

/**
 * Persist a single ledger entry to Redis.
 * Returns success boolean so the ingestion loop can track safe cursor advance.
 *
 * @param {object} entry
 * @returns {boolean} true if persisted successfully
 */
function _persistEntry(entry) {
  try {
    lineageLedger.recordWorkerEntry(entry);
    return true;
  } catch (err) {
    console.error('[lineage-worker] Failed to persist ledger entry:', err.message);
    return false;
  }
}

/**
 * Persist the safe cursor via the lineage ledger.
 * Uses _lastPersistedCursor — only advances when all entries in a tick persisted (A3).
 */
function _persistCursor() {
  lineageLedger.persistWorkerCursor(_lastPersistedCursor, HEALTH_TTL_S * 2);
}

/**
 * Emit a backpressure signal through the observability plane.
 * Governance discovers this by reading the ledger (worker ingests its own
 * transition on next tick, producing a ledger entry).
 */
function _emitBackpressure() {
  try {
    const observability = require('../observability');
    observability.transition({
      domain: 'governance',
      entity: 'worker',
      entityId: 'lineage-worker',
      previousState: 'HEALTHY',
      nextState: 'BACKPRESSURE',
      authority: 'lineage-worker',
      raw: {
        consecutiveFailures: _consecutiveFailures,
        lastPersistedCursor: _lastPersistedCursor,
        readCursor: _cursor,
      },
    });
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Layer B REMOVED — projection synthesis now owned by telemetry-workers.
// The lineage worker Layer A now consumes SEMANTIC_PROJECTION_TRANSITION entries
// from the observability plane and persists them to the ledger unchanged.
// Shared projection workers (5 workers in telemetry-workers/) now synthesize
// runtime projections and emit them as SEMANTIC_PROJECTION_TRANSITION.
// The lineage worker DOES NOT reinterpret these — it only validates and persists.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process lineage entries through the projection interpretation engine.
 * Updates in-memory projections for all 5 projection types.
 * Called after each Layer A ingestion tick.
 *
 * @param {Array<object>} newEntries — entries ingested in this tick
 */
function _projectTick(newEntries) {
  if (newEntries.length === 0) return;

  for (const entry of newEntries) {
    _projectDomain(entry);
    _projectGovernanceRuntime(entry);
    _projectHealth(entry);
    _projectAuthority(entry);
  }
  _projectIntegrity();

  // Update projection snapshot version
  _projections._meta = {
    projectionVersion: PROJECTION_VERSION,
    lineageVersion: LINEAGE_VERSION,
    updatedAt: Date.now(),
    entryCount: _entryCount,
    cursor: _cursor,
  };
}

/**
 * Project domain runtime state from FSM and domain-specific transitions.
 */
function _projectDomain(entry) {
  const { domain } = entry;
  if (!_projections.domain[domain]) return;

  const proj = _projections.domain[domain];
  proj.transitionCount++;
  proj.lastTransition = entry.timestamp;
  proj.state = entry.nextState;

  // Track state oscillation for instability detection
  const stateKey = `${domain}:fsm:${entry.entityId || domain + '-fsm'}`;
  if (!_stateHistory.has(stateKey)) _stateHistory.set(stateKey, []);
  const history = _stateHistory.get(stateKey);
  history.push({ state: entry.nextState, ts: entry.timestamp });
  if (history.length > 20) history.splice(0, history.length - 20);

  // Detect oscillation: same state appeared 3+ times without progressing
  _updateOscillation(proj, history);
}

function _updateOscillation(proj, history) {
  if (history.length < 4) return;
  const recent = history.slice(-4);
  const states = recent.map(h => h.state);
  const unique = new Set(states);
  // Oscillation: cycling through same states without settling
  if (unique.size <= 2 && states[0] !== states[3]) {
    proj.authorityStability = Math.max(0, proj.authorityStability - 0.1);
  }
}

/**
 * Project governance runtime signals from governance:runtime transitions.
 * These are derived from immutable ledger entries — no observation-time deps.
 *
 * SIGNAL OWNERSHIP CONTRACT (CK):
 *   runtimeState, lastStateTransition, degradationSignals, epochCount,
 *   domainInstability, replayContinuity are LEDGER_DERIVABLE — owned by
 *   lineage-worker Layer B. governancePressure is OBSERVER_RELATIVE —
 *   owned by telemetry-workers and MUST NOT be computed here.
 */
function _projectGovernanceRuntime(entry) {
  if (entry.domain !== 'governance' || entry.entity !== 'runtime') return;

  const proj = _projections.governanceRuntime;
  proj.runtimeState = entry.nextState;
  proj.lastStateTransition = entry.timestamp;

  const raw = entry.raw?.raw || entry.raw || {};

  // Update degradation signals — map of observed substate names
  if (raw.substate) {
    proj.degradationSignals[raw.substate.toLowerCase()] = entry.timestamp;
  }

  // Domain instability: detect if blocked transitions exist in ledger
  if (raw.blocked) {
    proj.domainInstability++;
  }

  // Epoch tracking from ledger
  if (raw.epochId) {
    proj.epochCount++;
  }

  // replayContinuity: check if governance:runtime entries have gaps > 30s
  const govEntries = _lineageBuffer.filter(
    e => e.domain === 'governance' && e.entity === 'runtime'
  );
  if (govEntries.length >= 2) {
    const last = govEntries[govEntries.length - 1];
    const prev = govEntries[govEntries.length - 2];
    const gap = last.timestamp - prev.timestamp;
    proj.replayContinuity = gap > 30_000 ? 'gap_detected' : 'intact';
  }
}

/**
 * Project health signals from ledger entries.
 * Only LEDGER_DERIVABLE signals — recomputable from immutable lineage replay.
 *
 * SIGNAL OWNERSHIP CONTRACT (CK):
 *   transitionCount, lastTransition, executionHealth, authorityStability
 *   are LEDGER_DERIVABLE — owned by lineage-worker Layer B.
 *   failureRate, retryPressure, bufferPressure, quotaPressure, circuitBreakers,
 *   interpretationConfidence are OBSERVER_RELATIVE — owned by telemetry-workers.
 */
function _projectHealth(entry) {
  const proj = _projections.health;

  proj.transitionCount++;
  proj.lastTransition = entry.timestamp;

  // executionHealth derived from governance runtime state (ledger-derivable)
  const govState = _projections.governanceRuntime.runtimeState;
  if (govState === 'HALTED' || govState === 'ERROR') {
    proj.executionHealth = 'CRITICAL';
  } else if (govState === 'DEGRADED') {
    proj.executionHealth = 'DEGRADED';
  } else if (govState === 'RECOVERY') {
    proj.executionHealth = 'RECOVERING';
  } else {
    proj.executionHealth = 'STABLE';
  }
}

/**
 * Project authority continuity per domain.
 * Detects authority oscillation — rapid authority changes on same entity.
 */
function _projectAuthority(entry) {
  const { domain } = entry;
  if (!_projections.authority[domain]) return;

  const proj = _projections.authority[domain];
  proj.authorityCount++;
  proj.lastAuthority = entry.authority;

  // Track authority transitions per domain+entity for oscillation detection
  const authKey = `${domain}:${entry.entity}`;
  if (!_authorityHistory.has(authKey)) _authorityHistory.set(authKey, []);
  const history = _authorityHistory.get(authKey);
  history.push({ authority: entry.authority, ts: entry.timestamp });
  if (history.length > 15) history.splice(0, history.length - 15);

  // Detect authority oscillation: >2 different authorities in last 5 transitions
  if (history.length >= 5) {
    const recent = history.slice(-5);
    const uniqueAuthorities = new Set(recent.map(h => h.authority));
    if (uniqueAuthorities.size > 2) {
      proj.authorityOscillation = Math.min(1.0, proj.authorityOscillation + 0.2);
      proj.continuityStatus = 'oscillating';
      _recordInterpretationAnomaly('AUTHORITY_OSCILLATION', {
        domain,
        entity: entry.entity,
        authorityCount: uniqueAuthorities.size,
        authorities: [...uniqueAuthorities],
      });
    }
  }
}

/**
 * Project runtime integrity from ledger entries.
 * Only LEDGER_DERIVABLE signals — recomputable from immutable lineage replay.
 *
 * SIGNAL OWNERSHIP CONTRACT (CK):
 *   structuralAnomalyCount, replayAnomalyProbability, cadenceGapProbability
 *   are LEDGER_DERIVABLE — owned by lineage-worker Layer B.
 *   executionPressure, authorityInstability are OBSERVER_RELATIVE — owned by
 *   telemetry-workers and MUST NOT be computed here.
 */
function _projectIntegrity() {
  const proj = _projections.integrity;

  // Structural anomalies count — from divergence log
  proj.structuralAnomalyCount = _divergences.filter(d => d.category === 'structural').length;

  // Cadence gap probability: derived from ledger timestamps (not Date.now())
  // Look at last N consecutive governance:runtime entry pairs, find max gap
  const govEntries = _lineageBuffer
    .filter(e => e.domain === 'governance' && e.entity === 'runtime')
    .slice(-20); // last 20 governance entries
  if (govEntries.length >= 2) {
    let maxGap = 0;
    for (let i = 1; i < govEntries.length; i++) {
      const gap = govEntries[i].timestamp - govEntries[i - 1].timestamp;
      if (gap > maxGap) maxGap = gap;
    }
    proj.cadenceGapProbability = Math.min(1.0, maxGap / 120_000); // 2min → 1.0
  } else {
    proj.cadenceGapProbability = 0;
  }

  // Replay anomaly probability: if replayContinuity shows gap, elevated risk
  const govProj = _projections.governanceRuntime;
  proj.replayAnomalyProbability = govProj.replayContinuity === 'gap_detected' ? 0.7 : 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Divergence Detection — runtime continuity anomalies ONLY, NOT governance violations
// ═══════════════════════════════════════════════════════════════════════════════

function _recordStructuralAnomaly(type, details) {
  const anomaly = {
    id: crypto.randomUUID(),
    type,
    category: 'structural',
    timestamp: Date.now(),
    details,
  };
  _divergences.push(anomaly);
  // Cap at 200
  if (_divergences.length > 200) _divergences.splice(0, _divergences.length - 200);

  // Also record as ledger entry — governance discovers via ledger reading (B1)
  _recordDivergenceEntry(anomaly);
}

function _recordInterpretationAnomaly(type, details) {
  const anomaly = {
    id: crypto.randomUUID(),
    type,
    category: 'runtime_interpretation',
    timestamp: Date.now(),
    details,
  };
  _divergences.push(anomaly);
  if (_divergences.length > 200) _divergences.splice(0, _divergences.length - 200);

  // Also record as ledger entry — governance discovers via ledger reading (B1)
  _recordDivergenceEntry(anomaly);
}

/**
 * Write a divergence as a typed ledger entry.
 * Fire-and-forget — divergence recording does not block ingestion.
 */
function _recordDivergenceEntry(anomaly) {
  try {
    lineageLedger.recordWorkerEntry({
      ledgerId: crypto.randomUUID(),
      timestamp: anomaly.timestamp,
      ingestedAt: Date.now(),
      entryType: 'divergence',
      domain: 'governance',
      entity: 'divergence',
      entityId: anomaly.id,
      previousState: null,
      nextState: anomaly.type,
      authority: 'lineage-worker',
      divergenceCategory: anomaly.category,
      divergenceDetails: anomaly.details,
      projectionVersion: PROJECTION_VERSION,
      lineageVersion: LINEAGE_VERSION,
    });
  } catch (err) {
    console.error('[lineage-worker] Failed to persist divergence entry:', err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Persistence — cursor, health, snapshot
// ═══════════════════════════════════════════════════════════════════════════════

function _persistHealth() {
  const health = {
    status: _running ? 'healthy' : 'stopped',
    uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
    lastTick: _lastTick,
    tickCount: _tickCount,
    entryCount: _entryCount,
    cursor: _cursor,
    lastPersistedCursor: _lastPersistedCursor,
    consecutiveFailures: _consecutiveFailures,
    projectionVersion: PROJECTION_VERSION,
    lineageVersion: LINEAGE_VERSION,
  };
  lineageLedger.persistWorkerHealth(health, HEALTH_TTL_S);

  // Also record as ledger entry — governance discovers via ledger reading (B1)
  try {
    lineageLedger.recordWorkerEntry({
      ledgerId: crypto.randomUUID(),
      timestamp: Date.now(),
      ingestedAt: Date.now(),
      entryType: 'health',
      domain: 'governance',
      entity: 'worker',
      entityId: 'lineage-worker',
      previousState: null,
      nextState: _running ? 'healthy' : 'stopped',
      authority: 'lineage-worker',
      health: {
        uptimeMs: health.uptimeMs,
        tickCount: _tickCount,
        entryCount: _entryCount,
        cursor: _cursor,
        lastPersistedCursor: _lastPersistedCursor,
        consecutiveFailures: _consecutiveFailures,
      },
      projectionVersion: PROJECTION_VERSION,
      lineageVersion: LINEAGE_VERSION,
    });
  } catch (err) {
    console.error('[lineage-worker] Failed to persist health entry:', err.message);
  }
}

function _persistProjectionSnapshot() {
  // CK signal ownership validation — emit divergence if contract is violated
  try {
    const CK = require('./constitutional-kernel');
    const { valid, violations } = CK.validateProjectionSnapshot(_projections, 'lineage-worker');
    if (!valid) {
      _recordStructuralAnomaly('PROJECTION_SIGNAL_CONTRACT_VIOLATION', {
        violations,
        projectionVersion: PROJECTION_VERSION,
      });
    }
  } catch (_) {}

  lineageLedger.persistWorkerProjection(_projections, HEALTH_TTL_S * 2);

  // Also record as ledger entry — governance discovers via ledger reading (B1)
  try {
    lineageLedger.recordWorkerEntry({
      ledgerId: crypto.randomUUID(),
      timestamp: Date.now(),
      ingestedAt: Date.now(),
      entryType: 'projection_snapshot',
      domain: 'governance',
      entity: 'projection',
      entityId: 'lineage-worker',
      previousState: null,
      nextState: 'SNAPSHOT',
      authority: 'lineage-worker',
      projections: JSON.parse(JSON.stringify(_projections)),
      projectionVersion: PROJECTION_VERSION,
      lineageVersion: LINEAGE_VERSION,
    });
  } catch (err) {
    console.error('[lineage-worker] Failed to persist projection snapshot entry:', err.message);
  }
}

function _persistDivergences() {
  const recent = _divergences.slice(-50);
  lineageLedger.persistWorkerDivergences(recent, HEALTH_TTL_S * 3);
}

/**
 * Load persisted cursor from the lineage ledger on boot.
 */
async function _rehydrateCursor() {
  const cursor = await lineageLedger.getWorkerCursor();
  if (cursor > 0) {
    _cursor = cursor;
    _lastPersistedCursor = cursor;
    console.log(`[lineage-worker] Rehydrated cursor: ${_cursor}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Core tick — the polling loop body
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Read current buffer pressure from the observability plane.
 * Returns a value 0..1 representing how backlogged the lineage worker is.
 *   0 = caught up (cursor at log head)
 *   1 = max backlog (log is full and worker is far behind)
 *
 * @returns {number} pressure 0..1
 */
function _getBufferPressure() {
  try {
    const observability = require('../observability');
    const logSize = observability.query.getLogSize();
    const lag = logSize - _cursor;
    if (lag <= 0) return 0;
    return Math.min(1.0, lag / MAX_LOG_ENTRIES);
  } catch (_) {
    return 0;
  }
}

/**
 * Compute the adaptive poll interval based on current buffer pressure.
 * Called every tick so the poll rate responds to changing conditions.
 *
 * Pressure 0.0–0.2 (healthy):  MAX_POLL_MS (5000ms) — decelerate, conserve resources
 * Pressure 0.2–0.5 (elevated): linear between MAX_POLL_MS and MIN_POLL_MS
 * Pressure 0.5–1.0 (severe):   MIN_POLL_MS (500ms)  — drain as fast as possible
 *
 * @returns {number} poll interval in milliseconds
 */
function _computeAdaptivePollInterval() {
  const pressure = _getBufferPressure();

  if (pressure >= PRESSURE_THRESHOLD_HIGH) {
    // Severe backlog — drain as fast as possible
    return MIN_POLL_MS;
  }

  if (pressure <= PRESSURE_THRESHOLD_LOW) {
    // Healthy — can afford to slow down
    return MAX_POLL_MS;
  }

  // Elevated: linear interpolation between MAX and MIN
  const range = PRESSURE_THRESHOLD_HIGH - PRESSURE_THRESHOLD_LOW;
  const t = (pressure - PRESSURE_THRESHOLD_LOW) / range;
  const interval = MAX_POLL_MS - (t * (MAX_POLL_MS - MIN_POLL_MS));
  return Math.round(interval);
}

async function _tick() {
  _lastTick = Date.now();
  _tickCount++;

  // Recompute adaptive poll interval every tick
  const newPollMs = _computeAdaptivePollInterval();

  // Layer A: ingest transitions into immutable lineage
  const { consumed, rejected, truncated } = _ingestTick();

  // If buffer was truncated (eviction happened), max out drain speed immediately
  if (truncated && _currentPollMs > MIN_POLL_MS) {
    _currentPollMs = MIN_POLL_MS;
    _reschedulePollTimer(MIN_POLL_MS);
  } else if (newPollMs !== _currentPollMs) {
    _currentPollMs = newPollMs;
    _reschedulePollTimer(_currentPollMs);
  }

  // Persist health on every tick
  _persistHealth();

  if (consumed > 0 || _tickCount % 3 === 0) {
    // Uncomment for verbose debugging:
    // console.log(`[lineage-worker] tick ${_tickCount}: consumed=${consumed} rejected=${rejected} cursor=${_cursor} pressure=${_getBufferPressure().toFixed(3)} poll=${_currentPollMs}ms`);
  }
}

/**
 * Reschedule the poll timer with a new interval.
 * Called when adaptive logic determines the interval should change.
 *
 * @param {number} intervalMs — new poll interval
 */
function _reschedulePollTimer(intervalMs) {
  if (_pollTimer) {
    clearInterval(_pollTimer);
  }
  _pollTimer = setInterval(() => {
    _tick().catch(err => {
      console.error('[lineage-worker] Tick error:', err.message);
    });
  }, intervalMs);
  _pollTimer.unref();
  // console.log(`[lineage-worker] Poll interval adjusted → ${intervalMs}ms`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Start the lineage worker.
 * Registers as a consumer of the observability plane's transition log,
 * rehydrates the cursor from Redis, and begins the polling loop.
 *
 * @param {number} [pollIntervalMs=5000] — how often to poll for new transitions
 */
async function start(pollIntervalMs = DEFAULT_POLL_MS) {
  if (_running) {
    console.warn('[lineage-worker] Already running — ignoring duplicate start');
    return;
  }

  const observability = require('../observability');

  // Register as consumer for truncation protection
  observability.query.registerConsumer(CONSUMER_NAME);

  // Rehydrate cursor from Redis
  await _rehydrateCursor();

  // Rehydrate lineage buffer for causation chain validation across restarts.
  // _lineageBuffer.some() checks for parentTransitionId would false-positive
  // after a restart if the buffer is empty while the ledger has prior entries.
  try {
    const recentEntries = await lineageLedger.getWorkerLineage(500);
    if (recentEntries.length > 0) {
      _lineageBuffer.push(...recentEntries);
      console.log(`[lineage-worker] Rehydrated ${recentEntries.length} entries into lineage buffer`);
    }
  } catch (err) {
    console.warn('[lineage-worker] Could not rehydrate lineage buffer — causation checks may false-positive:', err.message);
  }

  _running = true;
  _startedAt = Date.now();
  _lastTick = Date.now();
  _currentPollMs = pollIntervalMs;

  // Initial tick to catch up on any entries produced before worker started
  await _tick();

  // Start the snapshot persistence timer
  _snapshotTimer = setInterval(() => {
    _persistProjectionSnapshot();
    _persistDivergences();
  }, SNAPSHOT_INTERVAL_MS);
  _snapshotTimer.unref();

  // Start the polling loop with adaptive interval
  _reschedulePollTimer(_currentPollMs);

  console.log(`[lineage-worker] Started — cursor=${_cursor}, poll=${_currentPollMs}ms, snapshot=${SNAPSHOT_INTERVAL_MS}ms`);
}

/**
 * Stop the lineage worker gracefully.
 * Persists final state, unregisters consumer, and clears timers.
 */
async function stop() {
  if (!_running) return;

  _running = false;

  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
  if (_snapshotTimer) {
    clearInterval(_snapshotTimer);
    _snapshotTimer = null;
  }

  // Final persistence
  _persistCursor();
  _persistHealth();
  _persistProjectionSnapshot();
  _persistDivergences();

  const observability = require('../observability');
  try {
    observability.query.unregisterConsumer(CONSUMER_NAME);
  } catch (_) {}

  console.log(`[lineage-worker] Stopped — ${_entryCount} entries ingested, cursor=${_cursor}`);
}

/**
 * Return the current projection snapshot.
 * Consumed by the reconciliation engine and governance systems.
 *
 * @returns {object}
 */
function getProjections() {
  return JSON.parse(JSON.stringify(_projections));
}

/**
 * Return worker health signals.
 *
 * @returns {{ status: string, uptimeMs: number, lastTick: number|null, tickCount: number, entryCount: number, cursor: number, projectionVersion: string, lineageVersion: string }}
 */
function getHealth() {
  return {
    status: _running ? 'healthy' : 'stopped',
    uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
    lastTick: _lastTick,
    tickCount: _tickCount,
    entryCount: _entryCount,
    cursor: _cursor,
    lastPersistedCursor: _lastPersistedCursor,
    consecutiveFailures: _consecutiveFailures,
    projectionVersion: PROJECTION_VERSION,
    lineageVersion: LINEAGE_VERSION,
  };
}

/**
 * Return detected runtime continuity anomalies.
 * Does NOT include constitutional violations — those belong to reconciliation.
 *
 * @returns {Array<object>}
 */
function getDivergences() {
  return [..._divergences];
}

/**
 * Return the number of ledger entries produced.
 *
 * @returns {number}
 */
function getLedgerSize() {
  return _entryCount;
}

/**
 * Notify waiters whose entryId has been consumed by the worker's cursor.
 * Called after each ingestion tick completes.
 */
function _notifyCommitWaiters() {
  for (const [entryId, waiter] of _commitWaiters) {
    // entryId is the numeric cursor position at the time the entry was emitted
    if (_cursor >= Number(entryId)) {
      clearTimeout(waiter.timeout);
      _commitWaiters.delete(entryId);
      waiter.resolve(entryId);
    }
  }
}

/**
 * Wait until the worker's cursor has advanced past the given entryId.
 * This provides deterministic constitutional visibility — the caller knows
 * when an emitted entry has been consumed and persisted, not just when it
 * was written to the observability plane.
 *
 * This eliminates timing-relative test barriers (sleep-based timeouts).
 *
 * @param {string|number} entryId — the ledger entry ID to wait for commit visibility
 * @param {number} [timeoutMs=30000] — maximum wait time; throws on timeout
 * @returns {Promise<string>} — resolves with the committed entryId
 * @throws {Error} — if timeout is exceeded
 */
function waitForCommit(entryId, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    // If already committed, resolve immediately
    if (_cursor >= Number(entryId)) {
      return resolve(entryId);
    }
    const timer = setTimeout(() => {
      _commitWaiters.delete(String(entryId));
      reject(new Error(`CommitTimeout: entry ${entryId} not committed within ${timeoutMs}ms (cursor=${_cursor})`));
    }, timeoutMs);
    _commitWaiters.set(String(entryId), { resolve, reject, timeout: timer });
  });
}

/**
 * Two-stage deterministic replay from a given cursor position.
 *
 * Stage 1: Immutable lineage reconstruction — deterministic, always same result.
 * Stage 2: Projection reinterpretation — uses current projection engine version.
 *
 * @param {number} fromCursor — starting cursor position for replay
 * @returns {{ entries: Array<object>, projections: object }}
 */
function replay(fromCursor) {
  const observability = require('../observability');
  const { entries } = observability.query.getEntriesSince(fromCursor || 0);

  const replayedEntries = [];

  // Stage 1: immutable lineage reconstruction
  for (const transition of entries) {
    const validation = _validateTransition(transition);
    if (!validation.valid) continue;
    replayedEntries.push(_transformToEntry(transition));
  }

  // Stage 2: projection reinterpretation using current engine
  // (for a full replay, we'd reset projections and reprocess — here we
  //  return what would be produced)
  const replayedProjections = JSON.parse(JSON.stringify(_projections));
  replayedProjections._meta = {
    projectionVersion: PROJECTION_VERSION,
    lineageVersion: LINEAGE_VERSION,
    replayFrom: fromCursor,
    replayedEntries: replayedEntries.length,
    replayedAt: Date.now(),
  };

  return { entries: replayedEntries, projections: replayedProjections };
}

module.exports = {
  start,
  stop,
  getProjections,
  getHealth,
  getDivergences,
  getLedgerSize,
  replay,
  waitForCommit,
  PROJECTION_VERSION,
  LINEAGE_VERSION,
};
