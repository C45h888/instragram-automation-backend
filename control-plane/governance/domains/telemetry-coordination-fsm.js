// control-plane/governance/domains/telemetry-coordination-fsm.js
// Deterministic Telemetry Coordination FSM: constitutional semantic ingress plane.
//
// Owns: semantic ingress ordering, projection ownership validation,
//        namespace authority validation, deterministic sequencing,
//        replay-safe serialization, projection eligibility gating,
//        membrane integrity enforcement, causal ingress continuity.
//
// Does NOT own: legality interpretation, governance decisions,
//               constitutional truth, replay conclusions,
//               reconciliation authority, runtime health interpretation.
//
// Architectural identity:
//   This FSM is a constitutional semantic traffic controller.
//   It coordinates ingress. It does NOT define truth.
//
// Reports to: constitutional kernel for transition validation + global observability.
// Signals HSM via ctx.dispatchGlobal() for backpressure and halt recommendations.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) recommends constitutional action
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Substrate ↓  → FSM reads intents from observability plane via deterministic cursors
//                  FSM emits validated transitions back through observability
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Telemetry workers no longer emit SEMANTIC_PROJECTION_TRANSITION directly.
// They emit PROJECTION_INTENT. This FSM is the sole serializer that validates,
// orders, and admits intents as canonical SEMANTIC_PROJECTION_TRANSITION.
//
// Topology:
//   projection workers → PROJECTION_INTENT → observability plane
//                                                   ↓
//   CK cadence → FSM reads intents → validates → orders → serializes
//                                                   ↓
//            SEMANTIC_PROJECTION_TRANSITION → observability → lineage worker → ledger
//
// Local states:
//   IDLE         — no coordination cycle in progress
//   VALIDATING   — reading intents from observability, validating ownership
//   ORDERING     — deterministically ordering validated intents
//   SERIALIZING  — transforming intents to canonical transitions
//   EMITTING     — emitting validated transitions to observability
//   HALTED       — CK-ordered halt, no processing allowed

const crypto = require('crypto');

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Governance Policy Constants — domain-owned thresholds
// ═══════════════════════════════════════════════════════════════════════════════

const MAX_BUFFERED_INTENTS = 100; // intent buffer saturation threshold

// Deterministic namespace ordering — ensures replay-stable serialization.
// Same intents always serialize in the same order, regardless of arrival timing.
const NAMESPACE_ORDER_PRIORITY = {
  integrity: 1,
  authority: 2,
  runtime: 3,
  health: 4,
  systemic: 5,
};
const DEFAULT_NAMESPACE_PRIORITY = 99;

// Known projection namespaces — only these may emit projection intents
const KNOWN_PROJECTION_NAMESPACES = new Set([
  'integrity', 'authority', 'runtime', 'health', 'systemic',
]);

// Signal ownership contract — maps projection payload signals to canonical owners.
// Derived from CK SIGNAL_OWNERSHIP_MAP for local validation. Only signals owned
// by 'telemetry-workers' may appear in projection intents.
const TELEMETRY_OWNED_SIGNALS = new Set([
  'health.failureRate',
  'health.retryPressure',
  'health.bufferPressure',
  'health.quotaPressure',
  'health.circuitBreakers',
  'health.interpretationConfidence',
  'health.runtimeEntropy',
  'health.operationalStress',
  'health.degradationSignals',
  'integrity.executionPressure',
  'governanceRuntime.governancePressure',
  'systemic.governancePressure',
  'systemic.systemicStress',
  'systemic.convergenceConfidence',
  'systemic.domainInstability',
]);

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No coordination cycle in progress — awaiting CK PROCESS_INTENTS tick',
  },
  HALTED: {
    description: 'CK-ordered halt — no intent processing allowed',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Process intents — CK cadence fires PROCESS_INTENTS ──────────────────
  PROCESS_INTENTS: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState === 'HALTED') {
        return { allowed: false, reason: 'Cannot process intents while HALTED' };
      }
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot process intents from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const actions = [];

      try {
        // 1. Read intents from observability plane
        const { intents, newCursor } = _readIntents();
        _intentCursor = newCursor;

        if (intents.length === 0) {
          _priorCycleOutputCount = 0;
          return [{ type: 'COORDINATION_NO_INTENTS', cursor: _intentCursor }];
        }

        // Backpressure check
        if (intents.length > MAX_BUFFERED_INTENTS) {
          _backpressureSignaled = true;
          if (ctx && ctx.dispatchGlobal) {
            ctx.dispatchGlobal({
              type: 'BACKPRESSURE_DETECTED',
              reason: `Coordination FSM intent buffer saturated: ${intents.length} pending (threshold: ${MAX_BUFFERED_INTENTS})`,
              evidence: { pendingIntentCount: intents.length },
            });
          }
        }

        // 2. Validate intents
        const { valid, rejected } = _validateIntents(intents);
        _rejectedIntentCount = rejected.length;

        if (valid.length === 0) {
          actions.push({
            type: 'COORDINATION_ALL_REJECTED',
            rejectedCount: rejected.length,
            violations: rejected.slice(0, 10),
          });
          return actions;
        }

        // 3. Deterministically order
        const ordered = _orderIntents(valid);

        // 4. Serialize to canonical transitions
        const transitions = ordered.map(intent => _serializeIntent(intent));
        _serializedTransitionCount = transitions.length;

        // 5. Emit validated transitions to observability
        let emittedCount = 0;
        for (const t of transitions) {
          const emitted = _emitTransition(t);
          if (emitted) emittedCount++;
        }

        _priorCycleOutputCount = emittedCount;

        actions.push({
          type: 'COORDINATION_CYCLE_COMPLETE',
          readCount: intents.length,
          validatedCount: valid.length,
          rejectedCount: rejected.length,
          emittedCount,
          cursor: _intentCursor,
        });

        if (_backpressureSignaled && emittedCount > 0) {
          _backpressureSignaled = false;
          if (ctx && ctx.dispatchGlobal) {
            ctx.dispatchGlobal({
              type: 'BACKPRESSURE_CLEARED',
              reason: `Coordination FSM processed ${emittedCount} intents — buffer drained`,
            });
          }
        }
      } catch (err) {
        actions.push({
          type: 'COORDINATION_CYCLE_ERROR',
          error: err.message,
        });
      }

      return actions;
    },
  },

  // ── Halt — CK orders immediate stop ─────────────────────────────────────
  HALT_TELEMETRY_COORDINATION: {
    target: 'HALTED',
    guard: (event) => {
      if (_localState === 'HALTED') {
        return { allowed: false, reason: 'Already HALTED' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      _priorCycleOutputCount = 0;
      return [{
        type: 'COORDINATION_HALTED',
        reason: event.reason || 'CK-ordered halt',
      }];
    },
  },

  // ── Resume — CK orders resume from halt ─────────────────────────────────
  RESUME_TELEMETRY_COORDINATION: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'HALTED') {
        return { allowed: false, reason: `Cannot resume from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{
      type: 'COORDINATION_RESUMED',
    }],
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ── Cursor — deterministic read position in observability transition log
let _intentCursor = 0;

// ── Cycle counters
let _cycleCount = 0;
let _rejectedIntentCount = 0;
let _serializedTransitionCount = 0;
let _priorCycleOutputCount = 0;
let _backpressureSignaled = false;

// ── Rejection log — last N rejected intents for forensic analysis
const _rejectionLog = []; // capped at 50
const MAX_REJECTION_LOG = 50;

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Intent Processing — pure functions, no timing/async dependencies
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Read PROJECTION_INTENT entries from the observability transition log.
 * Uses deterministic cursor positions — same log, same positions, same output.
 *
 * @returns {{ intents: Array<object>, newCursor: number }}
 */
function _readIntents() {
  try {
    const observability = require('../../observability');
    const { entries, nextCursor } = observability.query.getEntriesSince(_intentCursor);

    // Filter: only PROJECTION_INTENT entries
    const intents = [];
    for (const entry of entries) {
      if (entry.nextState === 'PROJECTION_INTENT') {
        intents.push(entry);
      }
    }

    return { intents, newCursor: nextCursor };
  } catch (err) {
    console.error('[telemetry-coordination-fsm] Failed to read intents:', err.message);
    return { intents: [], newCursor: _intentCursor };
  }
}

/**
 * Validate a single projection intent against constitutional contracts.
 * Pure function — same intent always produces same result.
 *
 * @param {object} intent — raw PROJECTION_INTENT entry from observability
 * @returns {{ valid: boolean, violations: Array<{ field: string, reason: string }> }}
 */
function _validateSingleIntent(intent) {
  const violations = [];
  const raw = intent.raw || {};

  // 1. Validate projection namespace is known
  const namespace = raw.projectionNamespace;
  if (!namespace || !KNOWN_PROJECTION_NAMESPACES.has(namespace)) {
    violations.push({
      field: 'projectionNamespace',
      reason: `Unknown projection namespace: '${namespace}'`,
    });
    return { valid: false, violations };
  }

  // 2. Validate authority — must be a projection worker
  const authority = intent.authority;
  if (!authority || !authority.includes('projection-worker')) {
    violations.push({
      field: 'authority',
      reason: `Invalid projection authority: '${authority}'`,
    });
    return { valid: false, violations };
  }

  // 3. Validate projection payload exists
  const payload = raw.projectionPayload;
  if (!payload || typeof payload !== 'object') {
    violations.push({
      field: 'projectionPayload',
      reason: 'Missing or invalid projection payload',
    });
    return { valid: false, violations };
  }

  // 4. Validate projection type is in schema
  const projectionType = raw.projectionType;
  if (!projectionType || typeof projectionType !== 'string') {
    violations.push({
      field: 'projectionType',
      reason: 'Missing projection type',
    });
  }

  // 5. Validate signal ownership — projection payload signals must belong
  //    to telemetry-workers per SIGNAL_OWNERSHIP_MAP
  _validateSignalOwnership(payload, namespace, violations);

  return { valid: violations.length === 0, violations };
}

/**
 * Recursively validate that projection payload signals are owned by telemetry-workers.
 * Signals owned by 'lineage-worker' found in projection payloads indicate
 * a signal ownership contract violation.
 *
 * @param {object} payload — projection payload
 * @param {string} namespace — projection namespace (health, integrity, etc.)
 * @param {Array} violations — accumulator
 */
function _validateSignalOwnership(payload, namespace, violations) {
  if (!payload || typeof payload !== 'object') return;

  for (const [key, value] of Object.entries(payload)) {
    if (typeof value === 'function' || key.startsWith('_')) continue;

    if (value && typeof value === 'object' && !Array.isArray(value)) {
      _validateSignalOwnership(value, namespace, violations);
    } else if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
      const signalPath = `${namespace}.${key}`;

      // Check if this signal exists in the ownership contract
      if (TELEMETRY_OWNED_SIGNALS.has(signalPath)) {
        // Signal is legitimately owned by telemetry-workers — allowed
        continue;
      }

      // Check if this signal is explicitly owned by lineage-worker (violation)
      // Lineage-worker signals are LEDGER_DERIVABLE and must NOT appear
      // in telemetry projection payloads
      const lineageOwnedPrefixes = [
        'domain.', 'authority.acquisition.', 'authority.publishing.',
        'authority.scheduling.', 'governanceRuntime.runtimeState',
        'governanceRuntime.lastStateTransition', 'governanceRuntime.degradationSignals',
        'governanceRuntime.epochCount', 'governanceRuntime.domainInstability',
        'integrity.structuralAnomalyCount', 'integrity.replayAnomalyProbability',
        'integrity.cadenceGapProbability', 'health.transitionCount',
        'health.lastTransition', 'health.executionHealth', 'health.authorityStability',
      ];

      const isLineageOwned = lineageOwnedPrefixes.some(prefix => signalPath.startsWith(prefix));

      if (isLineageOwned) {
        violations.push({
          field: signalPath,
          reason: `Signal '${signalPath}' is ledger-derivable (owned by lineage-worker) — must not appear in telemetry projection payloads`,
        });
      }
      // Unknown signals (not in either ownership set) — allowed, may be newly classified
    }
  }
}

/**
 * Validate all intents. Returns validated and rejected lists.
 * Pure function — no side effects.
 *
 * @param {Array<object>} intents
 * @returns {{ valid: Array<object>, rejected: Array<{ intent: object, violations: Array }> }}
 */
function _validateIntents(intents) {
  const valid = [];
  const rejected = [];

  for (const intent of intents) {
    const result = _validateSingleIntent(intent);
    if (result.valid) {
      valid.push(intent);
    } else {
      rejected.push({ intent, violations: result.violations });
      _recordRejection(intent, result.violations);
    }
  }

  return { valid, rejected };
}

/**
 * Deterministically order validated intents by:
 *   1. Namespace priority (integrity > authority > runtime > health > systemic)
 *   2. Within same namespace: projectionType lexical order
 *   3. Within same projectionType: traceId lexical order
 *
 * This is a pure function — same input always produces same order.
 *
 * @param {Array<object>} intents — validated intents
 * @returns {Array<object>} ordered intents
 */
function _orderIntents(intents) {
  return [...intents].sort((a, b) => {
    const rawA = a.raw || {};
    const rawB = b.raw || {};

    const nsA = rawA.projectionNamespace || '';
    const nsB = rawB.projectionNamespace || '';

    const priorityA = NAMESPACE_ORDER_PRIORITY[nsA] || DEFAULT_NAMESPACE_PRIORITY;
    const priorityB = NAMESPACE_ORDER_PRIORITY[nsB] || DEFAULT_NAMESPACE_PRIORITY;

    if (priorityA !== priorityB) return priorityA - priorityB;

    const typeA = rawA.projectionType || '';
    const typeB = rawB.projectionType || '';
    if (typeA !== typeB) return typeA.localeCompare(typeB);

    const traceA = a.traceId || '';
    const traceB = b.traceId || '';
    return traceA.localeCompare(traceB);
  });
}

/**
 * Serialize a validated intent into a canonical SEMANTIC_PROJECTION_TRANSITION.
 * Deterministic traceId — hash of intent content, same input → same traceId.
 *
 * @param {object} intent — validated PROJECTION_INTENT entry
 * @returns {object} canonical SEMANTIC_PROJECTION_TRANSITION
 */
function _serializeIntent(intent) {
  const raw = intent.raw || {};

  // Deterministic traceId: SHA-256 of intent content — replay-stable
  const contentForHash = JSON.stringify({
    projectionNamespace: raw.projectionNamespace,
    projectionType: raw.projectionType,
    projectionVersion: raw.projectionVersion,
    projectionPayload: raw.projectionPayload,
    correlationId: intent.correlationId,
    timestamp: intent.timestamp,
  });
  const traceId = crypto.createHash('sha256').update(contentForHash).digest('hex');

  return {
    domain: raw.projectionNamespace,
    entity: 'semantic_projection',
    entityId: raw.projectionType,
    previousState: `${raw.projectionType}:coordinated`,
    nextState: `${raw.projectionType}:projected`,
    authority: 'telemetry-coordination-fsm',
    traceId,
    correlationId: intent.correlationId || null,
    causationId: intent.traceId || null, // links back to original intent traceId
    parentTransitionId: null,
    raw: {
      entryType: 'SEMANTIC_PROJECTION_TRANSITION',
      projectionId: crypto.randomUUID(),
      projectionType: raw.projectionType,
      projectionVersion: raw.projectionVersion || '1.0.0',
      projectionNamespace: raw.projectionNamespace,
      projectionPayload: raw.projectionPayload,
      confidence: raw.confidence,
      integrityScore: raw.integrityScore,
      sourceTelemetryWindow: raw.sourceTelemetryWindow,
      coordinatedBy: 'telemetry-coordination-fsm',
      originalIntentTraceId: intent.traceId,
    },
  };
}

/**
 * Emit a validated SEMANTIC_PROJECTION_TRANSITION through the observability plane.
 * Fire-and-forget — emission failure does not halt the FSM.
 *
 * @param {object} transition — serialized transition
 * @returns {boolean} true if emitted successfully
 */
function _emitTransition(transition) {
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: transition.domain,
        entity: transition.entity,
        entityId: transition.entityId,
        previousState: transition.previousState,
        nextState: transition.nextState,
        authority: transition.authority,
        traceId: transition.traceId,
        correlationId: transition.correlationId,
        causationId: transition.causationId,
        raw: transition.raw,
      });
      return true;
    }
  } catch (err) {
    console.error('[telemetry-coordination-fsm] Emission error:', err.message);
  }
  return false;
}

/**
 * Record a rejected intent in the forensic rejection log.
 *
 * @param {object} intent — rejected intent
 * @param {Array} violations — ownership/schema violations
 */
function _recordRejection(intent, violations) {
  _rejectionLog.push({
    id: crypto.randomUUID(),
    timestamp: Date.now(),
    intentTraceId: intent.traceId,
    intentDomain: intent.domain,
    intentAuthority: intent.authority,
    projectionNamespace: intent.raw?.projectionNamespace,
    projectionType: intent.raw?.projectionType,
    violations: violations.map(v => ({ field: v.field, reason: v.reason })),
  });

  if (_rejectionLog.length > MAX_REJECTION_LOG) {
    _rejectionLog.splice(0, _rejectionLog.length - MAX_REJECTION_LOG);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// The lineage worker consumes these transitions and writes to canonical ledger.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the telemetry coordination FSM.
 *
 * The FSM coordinates semantic ingress only. It does NOT:
 *   - declare constitutional truth
 *   - infer legality
 *   - mutate governance state
 *   - override reconciliation
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, actions?: Array, reason?: string }}
 */
function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _localState;

  // 1. Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // 2. Resolve target state
  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event) : rawTarget;

  // null target = no state change
  if (target === null) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  // 3. Ask constitutional kernel for transition approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 4. Materialize state
  const priorState = _localState;
  _localState = target;

  // 5. Build actions — synchronously validate, order, serialize, emit.
  //    PROCESS_INTENTS is a fire-and-forget cycle. All work happens inside
  //    buildActions. The FSM returns to IDLE immediately — there are no
  //    intermediate states because serialization is synchronous and
  //    deterministic.
  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];
  _cycleCount++;

  // 6. Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'telemetry',
        entity: 'fsm',
        entityId: 'telemetry-coordination-fsm',
        previousState: priorState,
        nextState: target,
        authority: 'telemetry-coordination-fsm',
        raw: {
          intent: event.type,
          cycleCount: _cycleCount,
          rejectedIntentCount: _rejectedIntentCount,
          serializedTransitionCount: _serializedTransitionCount,
          priorCycleOutputCount: _priorCycleOutputCount,
        },
      });
    }
  } catch (_) {}

  console.log(`[telemetry-coordination-fsm] ${priorState} → ${target}  (${event.type}) cycles=${_cycleCount}`);

  return {
    allowed: true,
    from: priorState,
    to: target,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'IDLE', 'HALTED')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[telemetry-coordination-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }

  // Bootstrap cursor from observability log size
  try {
    const observability = require('../../observability');
    _intentCursor = observability.query.getLogSize();
    console.log(`[telemetry-coordination-fsm] Bootstrapped cursor: ${_intentCursor}`);
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    intentCursor: _intentCursor,
    cycleCount: _cycleCount,
    rejectedIntentCount: _rejectedIntentCount,
    serializedTransitionCount: _serializedTransitionCount,
    priorCycleOutputCount: _priorCycleOutputCount,
    backpressureSignaled: _backpressureSignaled,
    rejectionLogSize: _rejectionLog.length,
  };
}

function getHealth() {
  return {
    ok: _localState !== 'HALTED',
    signals: {
      state: _localState,
      cycleCount: _cycleCount,
      backpressureSignaled: _backpressureSignaled,
      rejectionRate: _cycleCount > 0
        ? _rejectedIntentCount / Math.max(1, _rejectedIntentCount + _serializedTransitionCount)
        : 0,
    },
  };
}

/**
 * Return the rejection log for forensic analysis.
 *
 * @param {number} [n] — number of recent rejections (default: all)
 * @returns {Array<object>}
 */
function getRejectionLog(n) {
  if (typeof n === 'number' && n > 0) {
    return _rejectionLog.slice(-n);
  }
  return [..._rejectionLog];
}

module.exports = {
  name: 'telemetry-coordination',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getRejectionLog,
  MAX_BUFFERED_INTENTS,
};
