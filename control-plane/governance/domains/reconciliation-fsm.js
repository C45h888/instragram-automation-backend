// control-plane/governance/domains/reconciliation-fsm.js
// Reconciliation Domain FSM: federated state machine governing reconciliation lifecycle.
//
// Owns: reconciliation cycle lifecycle (IDLE → RECONCILING → CONVERGENT/DRIFTED),
//        drift counter persistence, escalation threshold evaluation,
//        convergence detection, epoch tracking.
// Does NOT own: three-reality comparison computation (engine substrate),
//               constitutional state transitions (HSM/CK owns DEGRADED/RECOVERY),
//               substrate query construction (CK bridge subscriber owns that).
//
// Reports to: constitutional kernel for transition validation + global observability.
// Signals HSM via ctx.dispatchGlobal() for escalation and recovery recommendations.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) recommends constitutional state changes
//                  HSM (CK) validates and decides — FSM never mutates CK state
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Substrate ↓  → reconciliation engine performs mechanical comparison only
//                  FSM governs lifecycle meaning, engine performs computation
//
// Domain FSMs emit state transitions through the observability plane.
// The lineage worker consumes from the observability plane and writes to the
// canonical lineage ledger. FSMs do NOT write to the lineage ledger directly.
//
// Local states:
//   IDLE         — no reconciliation cycle in progress
//   RECONCILING  — cycle started, awaiting engine results from CK bridge
//   CONVERGENT   — cycle complete, all three layers converged (no drift)
//   DRIFTED      — cycle complete, drift detected in one or more domains

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

const ESCALATION_THRESHOLD = 3;       // consecutive substrate-drift epochs before signaling HSM
const RECOVERY_CONVERGENCE_MIN = 2;   // consecutive converged epochs needed to signal recovery
const MULTI_DOMAIN_DRIFT_MIN = 2;     // minimum drifted domains for multi-domain escalation

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No reconciliation cycle in progress — awaiting next RECONCILIATION_TICK',
  },
  RECONCILING: {
    description: 'Reconciliation cycle active — engine comparison in flight via CK bridge',
  },
  CONVERGENT: {
    description: 'Cycle complete — all three layers (lineage, FSM, substrate) converge',
  },
  DRIFTED: {
    description: 'Cycle complete — drift detected, counters incremented, escalation evaluated',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map — event → target + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Cycle initiation — orchestrator timer fires RECONCILIATION_TICK ──────
  RECONCILIATION_TICK: {
    target: 'RECONCILING',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot begin reconciliation from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      // Reset per-cycle slate
      _cycleObservations = [];
      _cycleWorstSeverity = 0;
      _cycleDriftedDomains = [];

      return [{
        type: 'RECONCILIATION_CYCLE_STARTED',
        epochCount: _epochCount,
        escalationSignaled: _escalationSignaled,
      }];
    },
  },

  // ── Results received from CK bridge after engine comparison ────────────
  RECONCILIATION_RESULTS_RECEIVED: {
    target: (event) => {
      // Dynamic target based on worstSeverity in engine results
      const worstSeverity = event.worstSeverity || 0;
      if (event.hashMismatch) {
        _hashMismatchDetected = true;
      }
      return worstSeverity === 0 ? 'CONVERGENT' : 'DRIFTED';
    },
    guard: (event) => {
      if (_localState !== 'RECONCILING') {
        return { allowed: false, reason: `Cannot receive results outside RECONCILING (current: ${_localState})` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { observations = [], worstSeverity = 0, hash, hashMismatch } = event;

      // Store cycle results for RECONCILIATION_CYCLE_COMPLETE evaluation
      _cycleObservations = observations;
      _cycleWorstSeverity = worstSeverity;
      _cycleDriftedDomains = observations.filter(o => o.severity > 0).map(o => o.domain);
      _lastEpochHash = hash ? hash.slice(0, 16) : null;
      _hashMismatchDetected = !!hashMismatch;

      return [{
        type: 'RECONCILIATION_RESULTS_PROCESSED',
        worstSeverity,
        driftedDomainCount: _cycleDriftedDomains.length,
        hashMismatch,
      }];
    },
  },

  // ── Cycle completion — evaluate counters, signal HSM if needed ─────────
  RECONCILIATION_CYCLE_COMPLETE: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'CONVERGENT' && _localState !== 'DRIFTED') {
        return { allowed: false, reason: `Cannot complete cycle from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const actions = [];
      _epochCount++;

      const priorState = _localState;

      if (priorState === 'CONVERGENT') {
        // ── Convergence path ──────────────────────────────────────────
        _consecutiveConverged++;
        _consecutiveDrifted = 0;
        _resetCounters();

        // Signal HSM if previously escalated and convergence is sustained
        if (_escalationSignaled && _consecutiveConverged >= RECOVERY_CONVERGENCE_MIN) {
          actions.push({
            type: 'RECONCILIATION_SIGNAL_CLEAR',
            consecutiveConverged: _consecutiveConverged,
            epochCount: _epochCount,
          });
          _escalationSignaled = false;
        }
      } else {
        // ── Drifted path ─────────────────────────────────────────────
        _consecutiveDrifted++;
        _consecutiveConverged = 0;

        // Increment drift counters from observations
        for (const obs of _cycleObservations) {
          if (obs.severity >= 3) { // SUBSTRATE or higher
            _driftCounters.substrate++;
          } else if (obs.severity === 2) { // REPLAY
            _driftCounters.replay++;
          }
        }

        // Multi-domain drift check
        const multiDomainDrift = _cycleDriftedDomains.length >= MULTI_DOMAIN_DRIFT_MIN;

        // Check escalation threshold
        if (_driftCounters.substrate >= ESCALATION_THRESHOLD || multiDomainDrift) {
          const evidence = {
            driftCounters: { ..._driftCounters },
            consecutiveDrifted: _consecutiveDrifted,
            driftedDomains: [..._cycleDriftedDomains],
            observations: _cycleObservations.filter(o => o.severity >= 2),
            multiDomain: multiDomainDrift,
          };

          actions.push({
            type: 'RECONCILIATION_SIGNAL_ESCALATE',
            reason: multiDomainDrift
              ? `Multi-domain substrate drift: ${_cycleDriftedDomains.join(', ')}`
              : `Persistent substrate drift: ${_driftCounters.substrate}/${ESCALATION_THRESHOLD} epochs`,
            evidence,
          });

          _escalationSignaled = true;
          _resetCounters();
        }
      }

      // ── Always emit cycle summary ────────────────────────────────────
      actions.push({
        type: 'RECONCILIATION_EPOCH_COMPLETE',
        epochCount: _epochCount,
        priorState,
        worstSeverity: _cycleWorstSeverity,
        driftedDomainCount: _cycleDriftedDomains.length,
        driftCounters: { ..._driftCounters },
        consecutiveConverged: _consecutiveConverged,
        consecutiveDrifted: _consecutiveDrifted,
        escalationSignaled: _escalationSignaled,
        hashMismatch: _hashMismatchDetected,
      });

      // Clean up cycle-local state
      _cycleObservations = [];
      _cycleDriftedDomains = [];
      _hashMismatchDetected = false;

      return actions;
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ── Persistent counters (survive across cycles, rehydrated from lineage) ─────
let _driftCounters = { substrate: 0, replay: 0 };
let _epochCount = 0;
let _consecutiveConverged = 0;
let _consecutiveDrifted = 0;
let _escalationSignaled = false;

// ── Cycle-local slate (reset on RECONCILIATION_TICK) ─────────────────────────
let _cycleObservations = [];
let _cycleWorstSeverity = 0;
let _cycleDriftedDomains = [];
let _lastEpochHash = null;
let _hashMismatchDetected = false;

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch — process event, ask constitutional for validation, transition
//
// Domain FSMs emit through observability plane (not lineage ledger).
// The lineage worker consumes these transitions and writes to canonical ledger.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the reconciliation FSM.
 *
 * The FSM governs lifecycle only. For HSM-level signals (escalation/clear),
 * it uses ctx.dispatchGlobal() to RECOMMEND constitutional state changes.
 * The HSM (CK) validates via GLOBAL_TRANSITION_MAP guards and makes the final decision.
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
  _localState = target;

  // 5. Emit observability transition for domain FSM state change
  // Fire-and-forget — observability failures never affect domain FSM behavior.
  // The lineage worker consumes this transition and writes to the canonical ledger.
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'reconciliation',
        entity: 'fsm',
        entityId: 'reconciliation-fsm',
        previousState: from,
        nextState: target,
        authority: 'reconciliation-fsm',
        raw: {
          intent: event.type,
          epochCount: _epochCount,
          driftCounters: { ..._driftCounters },
          escalationSignaled: _escalationSignaled,
        },
      });
    }
  } catch (_) {}

  // 6. Build actions
  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];

  // 7. HSM signaling — FSM recommends, HSM decides
  //    Process RECONCILIATION_SIGNAL_ESCALATE and RECONCILIATION_SIGNAL_CLEAR
  //    actions by calling ctx.dispatchGlobal() with evidence packages.
  const filteredActions = [];
  for (const action of actions) {
    if (action.type === 'RECONCILIATION_SIGNAL_ESCALATE') {
      // FSM recommends DEGRADED — HSM validates via GLOBAL_TRANSITION_MAP guard
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'RECONCILIATION_DRIFT_DETECTED',
          reason: action.reason,
          evidence: action.evidence,
        });
      }
      // Do not pass this action to subscribers — it's been handled
    } else if (action.type === 'RECONCILIATION_SIGNAL_CLEAR') {
      // FSM recommends HEALTHY — HSM validates via GLOBAL_TRANSITION_MAP guard
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'RECONCILIATION_CLEARED',
          reason: `Convergence sustained across ${action.consecutiveConverged} epochs`,
          evidence: {
            consecutiveConverged: action.consecutiveConverged,
            epochCount: action.epochCount,
          },
        });
      }
      // Do not pass this action to subscribers — it's been handled
    } else {
      filteredActions.push(action);
    }
  }

  console.log(`[reconciliation-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actions: filteredActions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'IDLE', 'RECONCILING')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[reconciliation-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    driftCounters: { ..._driftCounters },
    epochCount: _epochCount,
    consecutiveConverged: _consecutiveConverged,
    consecutiveDrifted: _consecutiveDrifted,
    escalationSignaled: _escalationSignaled,
    lastEpochHash: _lastEpochHash,
  };
}

function getHealth() {
  return {
    ok: _driftCounters.substrate === 0 && _driftCounters.replay === 0,
    signals: {
      state: _localState,
      activeCycle: _localState === 'RECONCILING',
      escalationSignaled: _escalationSignaled,
      consecutiveDrifted: _consecutiveDrifted,
      consecutiveConverged: _consecutiveConverged,
    },
  };
}

// ── Reconciliation engine getters — expose domain state for three-reality comparison ──

function getDriftCounters() {
  return { ..._driftCounters };
}

function getEpochCount() {
  return _epochCount;
}

function getEscalationState() {
  return {
    signaled: _escalationSignaled,
    consecutiveDrifted: _consecutiveDrifted,
    consecutiveConverged: _consecutiveConverged,
    threshold: ESCALATION_THRESHOLD,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Internal helpers
// ═══════════════════════════════════════════════════════════════════════════════

function _resetCounters() {
  _driftCounters.substrate = 0;
  _driftCounters.replay = 0;
}

module.exports = {
  name: 'reconciliation',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getDriftCounters,
  getEpochCount,
  getEscalationState,
  ESCALATION_THRESHOLD,
  RECOVERY_CONVERGENCE_MIN,
};
