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
const SNAPSHOT_INTERVAL_MS = 15_000; // how often to persist projection snapshot
const HEALTH_TTL_S = 30; // worker health key TTL in Redis

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

// In-memory lineage buffer — consumed by Layer B for projection synthesis
const _lineageBuffer = []; // Array<ledgerEntry>

// Layer B — projection state (evolvable, versioned)
const _projections = {
  domain: {
    acquisition: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0, retryPressure: 0 },
    publishing: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    scheduling: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0, cadenceContinuity: 1.0 },
    dedup: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
    reconciliation: { state: 'IDLE', transitionCount: 0, lastTransition: null, authorityStability: 1.0 },
  },
  governanceRuntime: {
    runtimeState: 'BOOTING',
    degradationSignals: {},
    governancePressure: 0,
    replayContinuity: 'intact',
    domainInstability: 0,
    epochCount: 0,
    lastStateTransition: null,
  },
  health: {
    executionHealth: 'STABLE',
    retryPressure: 0,
    bufferPressure: 0,
    quotaPressure: 0,
    circuitBreakers: 0,
    failureRate: 0,
    interpretationConfidence: 1.0,
  },
  authority: {
    acquisition: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    publishing: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
    scheduling: { authorityCount: 0, lastAuthority: null, authorityOscillation: 0, continuityStatus: 'intact' },
  },
  integrity: {
    cadenceGapProbability: 0,
    replayAnomalyProbability: 0,
    authorityInstability: 0,
    executionPressure: 0,
    structuralAnomalyCount: 0,
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
  for (const field of REQUIRED_TRANSITION_FIELDS) {
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
 * These are observed runtime states, NOT constitutional legitimacy judgments.
 */
function _projectGovernanceRuntime(entry) {
  if (entry.domain !== 'governance' || entry.entity !== 'runtime') return;

  const proj = _projections.governanceRuntime;
  proj.runtimeState = entry.nextState;
  proj.lastStateTransition = entry.timestamp;

  const raw = entry.raw?.raw || entry.raw || {};

  // Update degradation signals
  if (raw.substate) {
    proj.degradationSignals[raw.substate.toLowerCase()] = entry.timestamp;
  }

  // Governance pressure: DEGRADED + RECOVERY → elevated
  if (entry.nextState === 'DEGRADED' || entry.nextState === 'RECOVERY') {
    proj.governancePressure = Math.min(1.0, proj.governancePressure + 0.3);
  } else if (entry.nextState === 'HEALTHY') {
    proj.governancePressure = Math.max(0, proj.governancePressure - 0.2);
  }

  // Domain instability: detect if blocked transitions exist
  if (raw.blocked) {
    proj.domainInstability++;
  }

  // Epoch tracking
  if (raw.epochId) {
    proj.epochCount++;
  }
}

/**
 * Project runtime health signals from execution, quota, buffer transitions.
 */
function _projectHealth(entry) {
  const proj = _projections.health;

  switch (entry.domain) {
    case 'execution': {
      if (entry.entity === 'attempt') {
        if (entry.nextState === 'FAILED') {
          proj.failureRate = Math.min(1.0, proj.failureRate + 0.05);
          proj.executionHealth = proj.failureRate > 0.3 ? 'DEGRADED' : 'PRESSURE';
        } else if (entry.nextState === 'COMPLETED') {
          proj.failureRate = Math.max(0, proj.failureRate - 0.02);
          if (proj.failureRate < 0.1) proj.executionHealth = 'STABLE';
        } else if (entry.nextState === 'RETRYING') {
          proj.retryPressure = Math.min(1.0, proj.retryPressure + 0.1);
        } else if (entry.nextState === 'SKIPPED') {
          proj.circuitBreakers++;
        }
      }
      break;
    }

    case 'quota': {
      if (entry.entity === 'quota') {
        if (entry.nextState === 'CRITICAL') proj.quotaPressure = 1.0;
        else if (entry.nextState === 'ELEVATED') proj.quotaPressure = 0.6;
        else if (entry.nextState === 'NORMAL') proj.quotaPressure = 0;
      } else if (entry.entity === 'circuit_breaker') {
        if (entry.nextState === 'RATE_LIMITED') proj.circuitBreakers++;
        else if (entry.nextState === 'IDLE') proj.circuitBreakers = Math.max(0, proj.circuitBreakers - 1);
      }
      break;
    }

    case 'buffer': {
      if (entry.nextState === 'BUFFERING' || entry.nextState === 'FLUSHING') {
        proj.bufferPressure = Math.min(1.0, proj.bufferPressure + 0.2);
      } else if (entry.nextState === 'EMPTY' || entry.nextState === 'DESTROYED') {
        proj.bufferPressure = Math.max(0, proj.bufferPressure - 0.15);
      }
      break;
    }
  }

  // Interpretation confidence degrades when signals are volatile
  const volatility = proj.retryPressure + proj.bufferPressure + proj.quotaPressure + proj.failureRate;
  proj.interpretationConfidence = Math.max(0.3, 1.0 - volatility * 0.3);
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
 * Project runtime integrity — interpretation signals for the reconciliation engine.
 * NOT constitutional convergence — the reconciliation engine independently verifies.
 */
function _projectIntegrity() {
  const proj = _projections.integrity;

  // Cadence gap probability: infer from time gap since last governance tick
  const govProj = _projections.governanceRuntime;
  const lastTick = govProj.lastStateTransition;
  if (lastTick) {
    const gap = Date.now() - lastTick;
    proj.cadenceGapProbability = Math.min(1.0, gap / 120_000); // 2min → 1.0
  }

  // Authority instability: max oscillation across domains
  const authValues = Object.values(_projections.authority).map(a => a.authorityOscillation);
  proj.authorityInstability = authValues.length > 0 ? Math.max(...authValues) : 0;

  // Execution pressure: composite of retry + failure
  const healthProj = _projections.health;
  proj.executionPressure = (healthProj.retryPressure + healthProj.failureRate) / 2;

  // Structural anomalies accumulate
  proj.structuralAnomalyCount = _divergences.filter(d => d.category === 'structural').length;
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

async function _tick() {
  _lastTick = Date.now();
  _tickCount++;

  // Layer A: ingest transitions into immutable lineage
  // Layer B removed — projection synthesis now in telemetry-workers/
  const { consumed, rejected } = _ingestTick();

  // Persist health on every tick
  _persistHealth();

  if (consumed > 0 || _tickCount % 3 === 0) {
    // Uncomment for verbose debugging:
    // console.log(`[lineage-worker] tick ${_tickCount}: consumed=${consumed} rejected=${rejected} cursor=${_cursor} entries=${_entryCount}`);
  }
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

  // Initial tick to catch up on any entries produced before worker started
  await _tick();

  // Start the snapshot persistence timer
  _snapshotTimer = setInterval(() => {
    _persistProjectionSnapshot();
    _persistDivergences();
  }, SNAPSHOT_INTERVAL_MS);
  _snapshotTimer.unref();

  // Start the polling loop
  _pollTimer = setInterval(() => {
    _tick().catch(err => {
      console.error('[lineage-worker] Tick error:', err.message);
    });
  }, pollIntervalMs);
  _pollTimer.unref();

  console.log(`[lineage-worker] Started — cursor=${_cursor}, poll=${pollIntervalMs}ms, snapshot=${SNAPSHOT_INTERVAL_MS}ms`);
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
  PROJECTION_VERSION,
  LINEAGE_VERSION,
};
