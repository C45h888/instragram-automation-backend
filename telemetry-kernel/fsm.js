// telemetry-kernel/fsm.js
// Deterministic Telemetry Coordination FSM: constitutional semantic ingress plane.
// Migrated from control-plane/governance/domains/telemetry-coordination-fsm.js
// with observability/redis/lineage-ledger paths adjusted for kernel location.
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
//   HALTED       — CK-ordered halt, no processing allowed
//   INGRESS_LAG_RETRYING    — lag detected, retry cadence active
//   INGRESS_ESCALATED        — 3+ retries in 60s window
//   INGRESS_DEGRADED        — 6+ retries in 60s window, bounded worker dispatched

const { createRequire } = require('module');
const _require = createRequire(__filename);

const crypto = require('crypto');

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = _require('../../control-plane/observability/emitters/transition-emitter'); }
    catch (_) {}
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
  // ── Ingress lag retry escalation states ───────────────────────────────────
  INGRESS_LAG_RETRYING: {
    description: 'Ingress lag detected, retry cadence active via engagement FSM',
  },
  INGRESS_ESCALATED: {
    description: 'Retry budget partially consumed (3+ attempts in 60s), heightened monitoring',
  },
  INGRESS_DEGRADED: {
    description: 'Retry budget near exhaustion (6+ attempts in 60s), bounded worker dispatched, CK intervention required',
  },
};

const INTENT_NAMESPACES = Object.freeze(['runtime', 'integrity', 'authority', 'health', 'systemic']);

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
      if (_coordinationInFlight) {
        return { allowed: false, reason: 'Coordination cycle already in flight' };
      }
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot process intents from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      _coordinationInFlight = true;
      const cycle = _beginCoordinationCycle(event);

      try {
        // 1. Read intents from all domain-bounded partitions via per-namespace cursors
        const { intents, newCursors, dedupedCount } = await _readIntents();
        cycle.dedupedCount = dedupedCount;
        _intentCursors = newCursors;

        // Persist cursors to Redis after each coordination cycle.
        // Prevents replay storm on restart — FSM resumes from where it left off.
        const cursorPersistence = await _persistCursors(newCursors);

        if (intents.length === 0) {
          _clearBackpressure();
          _priorCycleOutputCount = 0;
          _completeCoordinationCycle({ emittedCount: 0, dedupedCount: 0 });
          return [{ type: 'COORDINATION_NO_INTENTS', cursors: _intentCursors }];
        }

        // Backpressure check
        _maybeSignalBackpressure(intents.length, ctx);

        // 2. Validate intents
        const { valid, rejected } = _validateIntents(intents);
        _rejectedIntentCount += rejected.length;

        if (valid.length === 0) {
          _clearBackpressure();
          _completeCoordinationCycle({ emittedCount: 0, dedupedCount: cycle.dedupedCount });
          return [{
            type: 'COORDINATION_ALL_REJECTED',
            rejectedCount: rejected.length,
            violations: rejected.slice(0, 10),
          }];
        }

        // 3. Deterministically order
        const ordered = _orderIntents(valid);

        // 4. Serialize to canonical transitions
        const transitions = ordered.map(intent => _serializeIntent(intent));
        _serializedTransitionCount += transitions.length;

        // 5. Emit validated transitions to observability
        let emittedCount = 0;
        for (const t of transitions) {
          const emitted = _emitTransition(t);
          if (emitted) emittedCount++;
        }

        _priorCycleOutputCount = emittedCount;
        _completeCoordinationCycle({ emittedCount, dedupedCount: cycle.dedupedCount });

        const actions = [{
          type: 'COORDINATION_CYCLE_COMPLETE',
          readCount: intents.length,
          validatedCount: valid.length,
          rejectedCount: rejected.length,
          emittedCount,
          dedupedCount: cycle.dedupedCount,
          cycleEpoch: cycle.epoch,
          cursors: _intentCursors,
        }];

        if (cursorPersistence.ok === false) {
          actions.push({
            type: 'LOG_DEGRADED',
            substate: 'CURSOR_PERSISTENCE_FAILED',
            reason: `Telemetry cursor persistence failed after ${cursorPersistence.attempts} attempts: ${cursorPersistence.error}`,
          });
        }

        if (_backpressureSignaled && emittedCount > 0) {
          _clearBackpressure();
          if (ctx && ctx.dispatchGlobal) {
            ctx.dispatchGlobal({
              type: 'BACKPRESSURE_CLEARED',
              reason: `Coordination FSM processed ${emittedCount} intents — buffer drained`,
            });
          }
        }

        return actions;
      } finally {
        _coordinationInFlight = false;
      }
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

  // ── Phase 3: Async constitutional validation (triggered by Phase 2 worker) ──
  PROJECTION_PERSISTED: {
    target: () => _localState,
    guard: (event) => {
      // Always allowed — async validation proceeds regardless of state
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const { ledgerId, entry } = event;
      const actions = [];

      try {
        // 1. Validate constitutionally (reuses existing intent validation)
        const validation = _validateSingleIntent({
          authority: entry.authority,
          raw: {
            projectionNamespace: entry.raw?.projectionNamespace,
            projectionType: entry.raw?.projectionType,
            projectionPayload: entry.raw?.projectionPayload,
            confidence: entry.raw?.confidence,
            integrityScore: entry.raw?.integrityScore,
          },
        });

        if (validation.valid) {
          // 2. Mark accepted in canonical ledger
          const lineageLedger = require('../../control-plane/governance/lineage-ledger');
          lineageLedger.markEntryAccepted(ledgerId).catch(err =>
            console.error('[telemetry-coordination-fsm] markEntryAccepted error:', err.message)
          );

          _serializedTransitionCount++;
          // PROJECTION_ACCEPTED action → interpreter subscribed via CK.subscribeAction
          actions.push({
            type: 'PROJECTION_ACCEPTED',
            ledgerId,
            entry: { ...entry, raw: { ...entry.raw, constitutionalStatus: 'ACCEPTED' } },
          });
        } else {
          // 4. Remove from canonical ledger
          const lineageLedger = require('../../control-plane/governance/lineage-ledger');
          lineageLedger.removeEntry(ledgerId).catch(err =>
            console.error('[telemetry-coordination-fsm] removeEntry error:', err.message)
          );

          // 5. Write REJECTION to anomaly log
          lineageLedger.recordAnomaly({
            type: 'REJECTED_PROJECTION',
            ledgerId,
            reason: validation.violations.map(v => v.reason).join('; '),
            violations: validation.violations,
            entry,
          }).catch(err =>
            console.error('[telemetry-coordination-fsm] recordAnomaly error:', err.message)
          );

          _rejectedIntentCount++;
          _recordRejection(entry, validation.violations);
          actions.push({ type: 'PROJECTION_REJECTED', ledgerId, violations: validation.violations });
        }
      } catch (err) {
        console.error('[telemetry-coordination-fsm] PROJECTION_PERSISTED error:', err.message);
        actions.push({ type: 'PROJECTION_VALIDATION_ERROR', error: err.message });
      }

      return actions;
    },
  },

  // ── Ingress lag retry orchestration (from CK) ────────────────────────────
  INGRESS_RETRY_REQUESTED: {
    target: (event) => {
      // Escalate based on retry budget
      if (_retryEscalationState === 'DEGRADED') return 'INGRESS_DEGRADED';
      if (_retryEscalationState === 'ESCALATED') return 'INGRESS_ESCALATED';
      return 'INGRESS_LAG_RETRYING';
    },
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      const { lag, status } = event;
      const actions = [];

      // ── Retry budget bookkeeping ──────────────────────────────────────
      const now = Date.now();
      const WINDOW_MS = 60_000;  // 60-second retry window
      const ESCALATION_THRESHOLD = 3;
      const DEGRADED_THRESHOLD = 6;

      // Reset window if expired
      if (_retryWindowStart === null || (now - _retryWindowStart) > WINDOW_MS) {
        _retryAttempts = 0;
        _retryWindowStart = now;
      }

      _retryAttempts++;
      _retryEscalationState =
        _retryAttempts >= DEGRADED_THRESHOLD ? 'DEGRADED' :
        _retryAttempts >= ESCALATION_THRESHOLD ? 'ESCALATED' :
        'RETRYING';

      // ── Coordination actions ──────────────────────────────────────────
      // Signal engagement FSM to activate retry cadence
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'RETRY_CADENCE_REQUEST',
          source: 'ingress',
          lag,
          escalationState: _retryEscalationState,
        });
      }

      // Log degradation
      actions.push({
        type: 'LOG_DEGRADED',
        substate: `INGRESS_LAG:${_retryEscalationState}`,
        reason: `Ingress lag ${lag}, retry attempt ${_retryAttempts}/${DEGRADED_THRESHOLD}`,
      });

      // Report to CK (via action — CK subscribes to this type)
      actions.push({
        type: 'INGRESS_RETRY_ACTIVE',
        lag,
        retryAttempts: _retryAttempts,
        escalationState: _retryEscalationState,
      });

      return actions;
    },
  },

  // ── Lag resolved — wind down retry budget ────────────────────────────────
  INGRESS_RESOLVED: {
    target: 'IDLE',
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      // Reset retry budget
      _retryAttempts = 0;
      _retryWindowStart = null;
      _retryEscalationState = 'IDLE';

      const actions = [];

      // Clear retry cadence in engagement FSM
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'RETRY_CADENCE_CLEAR',
          source: 'ingress',
        });
      }

      actions.push({
        type: 'LOG_DEGRADED',
        substate: 'INGRESS_RESOLVED',
        reason: 'Ingress lag cleared, retry cadence wound down',
      });

      return actions;
    },
  },

  // ── Transition Writer Health — worker layer degraded ─────────────────────
  TRANSITION_WRITER_HEALTH_CHANGED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      const { health } = event;
      const actions = [];
      _writerHealthSignal = health || null;
      _writerHealthState = !health ? 'UNKNOWN' : (health.ok ? 'OK' : health.status || 'DEGRADED');
      _writerHealthChangedAt = Date.now();

      if (health && !health.ok) {
        const worstCat = health.worstCategory;
        const escalationLevel =
          worstCat === 'REDIS_UNAVAILABLE' ? 'DEGRADED' :
          worstCat === 'CK_DISPATCH_FAILED' ? 'ESCALATED' :
          'RETRYING';

        if (ctx && ctx.dispatchGlobal) {
          ctx.dispatchGlobal({
            type: 'RETRY_CADENCE_REQUEST',
            source: 'worker',
            namespace: health.writers?.find(w => !w.ok)?.namespace ?? 'unknown',
            errorCategory: worstCat,
            lag: health.totalErrors || 0,
            escalationState: escalationLevel,
          });
        }

        const degradedWriter = health.writers?.find(w => !w.ok);
        actions.push({
          type: 'LOG_DEGRADED',
          substate: `WORKER_DEGRADED:${degradedWriter?.namespace ?? 'unknown'}`,
          reason: `Transition writer ${degradedWriter?.namespace ?? 'unknown'} degraded: [${worstCat}] ${degradedWriter?.lastError ?? 'unknown error'}`,
          errorCount: health.totalErrors,
          errorCategory: worstCat,
          failedWrites: degradedWriter?.failedWrites ?? 0,
        });

        actions.push({
          type: 'INGRESS_RETRY_ACTIVE',
          lag: health.totalErrors || 0,
          retryAttempts: 1,
          escalationState: escalationLevel,
          source: 'worker',
        });
      } else if (health && health.ok) {
        if (ctx && ctx.dispatchGlobal) {
          ctx.dispatchGlobal({
            type: 'RETRY_CADENCE_CLEAR',
            source: 'worker',
          });
        }

        actions.push({
          type: 'LOG_DEGRADED',
          substate: 'WORKER_RECOVERED',
          reason: `Transition writer layer recovered — total writes: ${health.totalWrites}`,
        });
      }

      return actions;
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _coordinationEpoch = 0;
let _coordinationInFlight = false;
let _lastCycleStartedAt = null;
let _lastCycleFinishedAt = null;

let _intentCursors = {
  runtime: 0,
  integrity: 0,
  authority: 0,
  health: 0,
  systemic: 0,
};

// ── Cycle counters
let _cycleCount = 0;
let _rejectedIntentCount = 0;
let _serializedTransitionCount = 0;
let _priorCycleOutputCount = 0;
let _priorCycleDedupedCount = 0;
let _backpressureSignaled = false;

// ── Ingress retry escalation budget
let _retryAttempts = 0;
let _retryWindowStart = null;
let _retryEscalationState = 'IDLE';

let _writerHealthState = 'UNKNOWN';
let _writerHealthChangedAt = null;
let _writerHealthSignal = null;
let _lastCursorPersistError = null;
let _cursorPersistenceFailures = 0;

// ── Default fail-open sanity check (universal gate pattern) ─────────────
// The ctx.sanityCheck is the universal gate. The FSM calls it
// during emission. For tests / non-CK dispatch, the default is
// always-allowed (fail-open to preserve operational cadence).
const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

// ── Rejection log
const _rejectionLog = [];
const MAX_REJECTION_LOG = 50;

// ═══════════════════════════════════════════════════════════════════════════════
// 3b. Reactive mode — onWrite event-driven coordination (Phase 3 trigger)
// ═══════════════════════════════════════════════════════════════════════════════

let _unsubscribeOnWrite = null;
let _coordinationPending = false;
let _reactiveCoordinationQueued = false;
let _ckContext = null;

function _onTransitionLogWrite(transition) {
  if (transition.nextState !== 'PROJECTION_INTENT') return;
  _triggerReactiveCoordination();
}

function _triggerReactiveCoordination() {
  if (_coordinationPending || _coordinationInFlight) {
    _reactiveCoordinationQueued = true;
    return;
  }
  _coordinationPending = true;

  setImmediate(() => {
    try {
      if (_ckContext && typeof _ckContext.dispatchGlobal === 'function') {
        _ckContext.dispatchGlobal({ type: 'TELEMETRY_PROCESS_INTENTS', source: 'reactive' });
      } else if (_ckContext && typeof _ckContext.dispatch === 'function') {
        _ckContext.dispatch({ type: 'PROCESS_INTENTS', source: 'reactive' });
      }
    } catch (err) {
      console.error('[telemetry-coordination-fsm] Reactive coordination error:', err.message);
    } finally {
      _coordinationPending = false;
      if (_reactiveCoordinationQueued) {
        _reactiveCoordinationQueued = false;
        _triggerReactiveCoordination();
      }
    }
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Intent Processing
// ═══════════════════════════════════════════════════════════════════════════════

async function _readIntents() {
  try {
    const observability = require('../../control-plane/observability/index.js');
    const allIntents = [];
    const newCursors = { ..._intentCursors };

    for (const namespace of INTENT_NAMESPACES) {
      const cursor = newCursors[namespace];
      const { entries, nextCursor } = await observability.query.getDomainEntriesSince(namespace, cursor);

      for (const entry of entries) {
        if (entry.nextState === 'PROJECTION_INTENT') {
          allIntents.push(entry);
        }
      }

      newCursors[namespace] = nextCursor;
    }

    const deduped = _dedupeIntents(allIntents);
    return {
      intents: deduped,
      newCursors,
      dedupedCount: Math.max(0, allIntents.length - deduped.length),
    };
  } catch (err) {
    console.error('[telemetry-coordination-fsm] Failed to read intents:', err.message);
    return { intents: [], newCursors: _intentCursors, dedupedCount: 0 };
  }
}

async function _persistCursors(newCursors) {
  const attempts = 3;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const redis = require('../../control-plane/config/redis').getRedisClient();
      if (!redis || redis.status !== 'ready') {
        _lastCursorPersistError = 'redis_not_ready';
        return { ok: false, attempts: attempt, error: _lastCursorPersistError };
      }

      const pipeline = redis.pipeline();
      for (const [namespace, cursor] of Object.entries(newCursors)) {
        pipeline.set(`governance:telemetry:fsm:cursor:${namespace}`, String(cursor), 'EX', 86400);
      }
      await pipeline.exec();
      _lastCursorPersistError = null;
      return { ok: true, attempts: attempt, error: null };
    } catch (err) {
      _lastCursorPersistError = err.message;
      if (attempt >= attempts) {
        _cursorPersistenceFailures++;
        console.error('[telemetry-coordination-fsm] _persistCursors error:', err.message);
        return { ok: false, attempts: attempt, error: err.message };
      }
      await _sleep(25 * attempt);
    }
  }
  return { ok: false, attempts, error: _lastCursorPersistError || 'unknown_cursor_persistence_error' };
}

async function _restoreCursors() {
  try {
    const redis = require('../../control-plane/config/redis').getRedisClient();
    if (!redis || redis.status !== 'ready') return null;

    const result = {};
    for (const ns of INTENT_NAMESPACES) {
      const saved = await redis.get(`governance:telemetry:fsm:cursor:${ns}`);
      result[ns] = saved ? parseInt(saved, 10) : 0;
    }
    return result;
  } catch (err) {
    return null;
  }
}

function _validateSingleIntent(intent) {
  const violations = [];
  const raw = intent.raw || {};

  const namespace = raw.projectionNamespace;
  if (!namespace || !KNOWN_PROJECTION_NAMESPACES.has(namespace)) {
    violations.push({
      field: 'projectionNamespace',
      reason: `Unknown projection namespace: '${namespace}'`,
    });
    return { valid: false, violations };
  }

  const authority = intent.authority;
  if (!authority || !authority.includes('projection-worker')) {
    violations.push({
      field: 'authority',
      reason: `Invalid projection authority: '${authority}'`,
    });
    return { valid: false, violations };
  }

  const payload = raw.projectionPayload;
  if (!payload || typeof payload !== 'object') {
    violations.push({
      field: 'projectionPayload',
      reason: 'Missing or invalid projection payload',
    });
    return { valid: false, violations };
  }

  const projectionType = raw.projectionType;
  if (!projectionType || typeof projectionType !== 'string') {
    violations.push({
      field: 'projectionType',
      reason: 'Missing projection type',
    });
  }

  _validateSignalOwnership(payload, namespace, violations);

  return { valid: violations.length === 0, violations };
}

function _validateSignalOwnership(payload, namespace, violations) {
  if (!payload || typeof payload !== 'object') return;

  for (const [key, value] of Object.entries(payload)) {
    if (typeof value === 'function' || key.startsWith('_')) continue;

    if (value && typeof value === 'object' && !Array.isArray(value)) {
      _validateSignalOwnership(value, namespace, violations);
    } else if (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean') {
      const signalPath = `${namespace}.${key}`;

      if (TELEMETRY_OWNED_SIGNALS.has(signalPath)) {
        continue;
      }

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
    }
  }
}

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

    const timeA = _getIntentTimestamp(a);
    const timeB = _getIntentTimestamp(b);
    if (timeA !== timeB) return timeA - timeB;

    const corrA = _getIntentCorrelationId(a);
    const corrB = _getIntentCorrelationId(b);
    if (corrA !== corrB) return corrA.localeCompare(corrB);

    const traceA = _getIntentTraceId(a);
    const traceB = _getIntentTraceId(b);
    return traceA.localeCompare(traceB);
  });
}

function _serializeIntent(intent) {
  const raw = intent.raw || {};
  const correlationId = _getIntentCorrelationId(intent);
  const causationId = _getIntentTraceId(intent);
  const timestamp = _getIntentTimestamp(intent);

  const contentForHash = JSON.stringify({
    projectionNamespace: raw.projectionNamespace,
    projectionType: raw.projectionType,
    projectionVersion: raw.projectionVersion,
    projectionPayload: raw.projectionPayload,
    correlationId,
    timestamp,
  });
  const traceId = crypto.createHash('sha256').update(contentForHash).digest('hex');
  const projectionId = traceId;

  return {
    domain: raw.projectionNamespace,
    entity: 'semantic_projection',
    entityId: raw.projectionType,
    previousState: `${raw.projectionType}:coordinated`,
    nextState: `${raw.projectionType}:projected`,
    authority: 'telemetry-coordination-fsm',
    traceId,
    correlationId,
    causationId,
    parentTransitionId: null,
    raw: {
      entryType: 'SEMANTIC_PROJECTION_TRANSITION',
      projectionId,
      projectionType: raw.projectionType,
      projectionVersion: raw.projectionVersion || '1.0.0',
      projectionNamespace: raw.projectionNamespace,
      projectionPayload: raw.projectionPayload,
      confidence: raw.confidence,
      integrityScore: raw.integrityScore,
      sourceTelemetryWindow: raw.sourceTelemetryWindow,
      originalIntentTraceId: causationId,
      coordinationEpoch: _coordinationEpoch,
    },
  };
}

function _dedupeIntents(intents) {
  const seen = new Set();
  const deduped = [];

  for (const intent of intents) {
    const identity = _getIntentIdentity(intent);
    if (seen.has(identity)) continue;
    seen.add(identity);
    deduped.push(intent);
  }

  return deduped;
}

function _getIntentIdentity(intent) {
  const raw = intent?.raw || {};
  return [
    raw.projectionNamespace || '',
    raw.projectionType || '',
    _getIntentCorrelationId(intent),
    _getIntentTraceId(intent),
    _getIntentTimestamp(intent),
  ].join('|');
}

function _getIntentTraceId(intent) {
  return intent?.traceId || intent?.raw?.traceId || '';
}

function _getIntentCorrelationId(intent) {
  return intent?.correlationId || intent?.raw?.correlationId || '';
}

function _getIntentTimestamp(intent) {
  return intent?.timestamp ||
    intent?.raw?.timestamp ||
    intent?.raw?.sourceTelemetryWindow?.closedAt ||
    0;
}

function _beginCoordinationCycle(event) {
  _coordinationEpoch++;
  _lastCycleStartedAt = Date.now();
  const cycle = {
    epoch: _coordinationEpoch,
    dedupedCount: 0,
    source: event?.source || 'cadence',
  };
  return cycle;
}

function _completeCoordinationCycle({ emittedCount, dedupedCount }) {
  _lastCycleFinishedAt = Date.now();
  if (typeof dedupedCount === 'number') {
    _priorCycleDedupedCount = dedupedCount;
  }
  _priorCycleOutputCount = emittedCount;
}

function _maybeSignalBackpressure(intentCount, ctx) {
  if (intentCount <= MAX_BUFFERED_INTENTS) return;
  _backpressureSignaled = true;
  if (ctx && ctx.dispatchGlobal) {
    ctx.dispatchGlobal({
      type: 'BACKPRESSURE_DETECTED',
      reason: `Coordination FSM intent buffer saturated: ${intentCount} pending (threshold: ${MAX_BUFFERED_INTENTS})`,
      evidence: { pendingIntentCount: intentCount },
    });
  }
}

function _clearBackpressure() {
  _backpressureSignaled = false;
}

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
// 5. Dispatch
// ═══════════════════════════════════════════════════════════════════════════════

function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _localState;

  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event) : rawTarget;

  if (target === null) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  const priorState = _localState;
  _localState = target;
  _cycleCount++;
  _emitFsmTransition(priorState, target, event);

  const actionResult = txn.buildActions ? txn.buildActions(event, ctx) : [];
  if (actionResult && typeof actionResult.then === 'function') {
    return actionResult.then((actions) => {
      console.log(`[telemetry-coordination-fsm] ${priorState} → ${target}  (${event.type}) cycles=${_cycleCount}`);
      return {
        allowed: true,
        from: priorState,
        to: target,
        actions: Array.isArray(actions) ? actions : [],
      };
    }).catch((err) => {
      console.error('[telemetry-coordination-fsm] buildActions error:', err.message);
      return {
        allowed: true,
        from: priorState,
        to: target,
        actions: [{
          type: 'LOG_DEGRADED',
          substate: 'COORDINATION_ERROR',
          reason: err.message,
        }],
      };
    });
  }

  console.log(`[telemetry-coordination-fsm] ${priorState} → ${target}  (${event.type}) cycles=${_cycleCount}`);
  return {
    allowed: true,
    from: priorState,
    to: target,
    actions: Array.isArray(actionResult) ? actionResult : [],
  };
}

function _emitFsmTransition(priorState, target, event) {
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
          coordinationEpoch: _coordinationEpoch,
          rejectedIntentCount: _rejectedIntentCount,
          serializedTransitionCount: _serializedTransitionCount,
          priorCycleOutputCount: _priorCycleOutputCount,
        },
      });
    }
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Initialization
// ═══════════════════════════════════════════════════════════════════════════════

async function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[telemetry-coordination-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }

  try {
    const observability = require('../../control-plane/observability/index.js');
    await observability.query.registerConsumer('telemetry-coordination-fsm');

    const restored = await _restoreCursors();
    if (restored) {
      _intentCursors = restored;
      console.log(`[telemetry-coordination-fsm] Consumer registered — cursors restored: ${JSON.stringify(_intentCursors)}, log size ${observability.query.getLogSize()}`);
    } else {
      _intentCursors = _emptyIntentCursors();
      console.log(`[telemetry-coordination-fsm] Consumer registered — cursors reset to 0, log size ${observability.query.getLogSize()}`);
    }
  } catch (_) {
    console.warn('[telemetry-coordination-fsm] Failed to register consumer:', _.message);
    _intentCursors = _emptyIntentCursors();
  }
}

function start(ctx) {
  if (_unsubscribeOnWrite) {
    console.log('[telemetry-coordination-fsm] Already started — skipping');
    return;
  }

  _ckContext = ctx || null;

  const observability = require('../../control-plane/observability/index.js');
  _unsubscribeOnWrite = observability.onWrite(_onTransitionLogWrite);

  console.log('[telemetry-coordination-fsm] Reactive mode active — onWrite subscription registered');
}

function stop() {
  if (_unsubscribeOnWrite) {
    try { _unsubscribeOnWrite(); } catch (_) {}
    _unsubscribeOnWrite = null;
    console.log('[telemetry-coordination-fsm] onWrite subscription stopped');
  }
  _coordinationPending = false;
  _coordinationInFlight = false;
  _reactiveCoordinationQueued = false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Observability
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    intentCursors: _intentCursors,
    cycleCount: _cycleCount,
    coordinationEpoch: _coordinationEpoch,
    coordinationInFlight: _coordinationInFlight,
    lastCycleStartedAt: _lastCycleStartedAt,
    lastCycleFinishedAt: _lastCycleFinishedAt,
    rejectedIntentCount: _rejectedIntentCount,
    serializedTransitionCount: _serializedTransitionCount,
    priorCycleOutputCount: _priorCycleOutputCount,
    priorCycleDedupedCount: _priorCycleDedupedCount,
    backpressureSignaled: _backpressureSignaled,
    writerHealthState: _writerHealthState,
    writerHealthChangedAt: _writerHealthChangedAt,
    cursorPersistenceFailures: _cursorPersistenceFailures,
    lastCursorPersistError: _lastCursorPersistError,
    rejectionLogSize: _rejectionLog.length,
  };
}

function getHealth() {
  return {
    ok: _localState !== 'HALTED',
    signals: {
      state: _localState,
      cycleCount: _cycleCount,
      coordinationInFlight: _coordinationInFlight,
      backpressureSignaled: _backpressureSignaled,
      writerHealthState: _writerHealthState,
      cursorPersistenceFailures: _cursorPersistenceFailures,
      rejectionRate: _cycleCount > 0
        ? _rejectedIntentCount / Math.max(1, _rejectedIntentCount + _serializedTransitionCount)
        : 0,
    },
  };
}

function getRejectionLog(n) {
  if (typeof n === 'number' && n > 0) {
    return _rejectionLog.slice(-n);
  }
  return [..._rejectionLog];
}

function getIngressRetryState() {
  return {
    retryAttempts: _retryAttempts,
    windowStart: _retryWindowStart,
    escalationState: _retryEscalationState,
    localState: _localState,
  };
}

function _emptyIntentCursors() {
  return {
    runtime: 0,
    integrity: 0,
    authority: 0,
    health: 0,
    systemic: 0,
  };
}

module.exports = {
  name: 'telemetry-coordination-fsm',
  dispatch,
  init,
  start,
  stop,
  getState,
  exportState,
  getHealth,
  getRejectionLog,
  getIngressRetryState,
  MAX_BUFFERED_INTENTS,
};
