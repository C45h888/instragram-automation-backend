// control-plane/governance/constitutional-kernel.js
// Constitutional Kernel: arbiter of runtime legitimacy and invariant law.
//
// Owns: global lifecycle (BOOTING/HEALTHY/DEGRADED/RECOVERY/HALTED),
//        general guards, domain registration and coordination,
//        cross-domain transition validation, watchdog staleness detection,
//        action subscription fabric, unified lineage ledger (canonical truth).
//
// Does NOT own: domain lifecycle states, execution intelligence,
//               retry decisions, buffer mechanics, evaluation policy,
//               scheduling logic — those belong to domain FSMs.
//
// Architectural invariant:
//   Signals UP     → dispatch(event) routes to domain FSMs or handles globals
//   Authority DOWN → validateDomainTransition() approves/rejects domain transitions
//                   subscribeAction() emits governance actions to membranes
//   Lineage        → all events (constitutional + domain) write here FIRST
//                   lineage is canonical truth; runtime state is a projection
//
// Domain FSMs are registered at boot. Each FSM conforms to the domain contract:
//   fsm.name              — unique domain identifier
//   fsm.dispatch(event, ctx) → { allowed, actions, lineageId }
//   fsm.getState()          → domain-local state string
//   fsm.exportState()       → domain state for observability
//   fsm.getHealth()         → health signals for degradation detection
//   fsm.init(state)          → (optional) called by CK on boot with rehydrated state
//
// This is the ONLY entry point for governance events. No subsystem may
// bypass the constitutional kernel. Domain FSMs write lineage via CK mediation
// (ctx.recordLineage) — they never directly access the lineage ledger.

const lineageLedger = require('./lineage-ledger');
const reconciliationEngine = require('./reconciliation-engine');

// Lazy import to avoid circular dependency at module load time
let _observabilityTransition = null;
function _getObservabilityTransition() {
  if (!_observabilityTransition) {
    try {
      _observabilityTransition = require('../observability/emitters/transition-emitter');
    } catch (_) {
      _observabilityTransition = null;
    }
  }
  return _observabilityTransition;
}

const STARTED_AT = Date.now();

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Event → Domain routing map
// ═══════════════════════════════════════════════════════════════════════════════

// Events NOT in this map are handled as global constitutional events.
const DOMAIN_EVENT_MAP = {
  // Acquisition domain
  ACQUISITION_INTENT_RECEIVED: 'acquisition',
  ACQUISITION_EXECUTING: 'acquisition',
  ACQUISITION_COMPLETE: 'acquisition',
  EXECUTION_OBSERVATION: 'acquisition',
  AUTH_FAILURE_STRIKE: 'acquisition',
  RATE_LIMIT_DETECTED: 'acquisition',
  RETRY_EXHAUSTED: 'acquisition',

  // Publishing domain
  BUFFER_EVENT_INGESTED: 'publishing',
  BUFFER_FLUSH_READY: 'publishing',
  EMISSION_OBSERVATION: 'publishing',

  // Scheduling domain
  CADENCE_TICK: 'scheduling',
  WORKER_METRICS_REPORTED: 'scheduling',
  DATABASE_SCANNED: 'scheduling',
  LIFECYCLE_REFRESHED: 'scheduling',
  SAFETY_CHECK_COMPLETE: 'scheduling',
};

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Global State Registry — flat constitutional lifecycle
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  BOOTING: {
    description: 'Runtime is initializing — modules, connections provisioning',
  },
  HEALTHY: {
    description: 'All governance domains operating within normal parameters',
  },
  DEGRADED: {
    description: 'One or more governance domains reporting degradation',
  },
  RECOVERY: {
    description: 'Runtime is recovering from a degraded or halted state',
  },
  HALTED: {
    description: 'Runtime has halted — manual intervention required',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 1b. State TTL — maximum duration before watchdog auto-recovers
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_TTL_MS = {
  BOOTING: 60_000,
  HEALTHY: Infinity,
  DEGRADED: 120_000,
  RECOVERY: 60_000,
  HALTED: Infinity,
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Global Transition Map — constitutional lifecycle transitions only
// ═══════════════════════════════════════════════════════════════════════════════

const GLOBAL_TRANSITION_MAP = {
  BOOT_COMPLETE: {
    target: 'HEALTHY',
    buildActions: () => [{ type: 'START_INTENT_DISCOVERY' }],
  },

  FATAL_ERROR: {
    target: 'HALTED',
    buildActions: (event) => [{
      type: 'LOG_HALT',
      reason: event.reason || 'Unspecified fatal error',
    }],
  },

  // ── Degradation ────────────────────────────────────────────────────────
  BACKPRESSURE_DETECTED: {
    target: 'DEGRADED',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY') {
        return { allowed: false, reason: `Backpressure only from HEALTHY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'BACKPRESSURE',
      reason: event.reason || 'Buffer accumulation exceeding capacity',
    }],
  },

  BACKPRESSURE_CLEARED: {
    target: 'HEALTHY',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Backpressure not active (currently ${ctx.state})` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  RETRY_PRESSURE_DETECTED: {
    target: 'DEGRADED',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY' && ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Retry pressure only from HEALTHY or DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'RETRY_PRESSURE',
      reason: event.reason || 'Worker retry rate elevated',
    }],
  },

  SIGNAL_DESYNC_DETECTED: {
    target: 'DEGRADED',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY' && ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Signal desync only from HEALTHY or DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'SIGNAL_DESYNC',
      reason: event.reason || 'Signal intake desynchronized',
    }],
  },

  PRESSURE_CLEARED: {
    target: 'HEALTHY',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Pressure clear only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // ── Recovery ───────────────────────────────────────────────────────────
  RECOVERY_INITIATED: {
    target: 'RECOVERY',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Recovery only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{
      type: 'LOG_RECOVERY',
      substate: 'RECONCILING',
    }],
  },

  RECOVERY_COMPLETE: {
    target: 'HEALTHY',
    guard: (event, ctx) => {
      if (ctx.state !== 'RECOVERY') {
        return { allowed: false, reason: `Recovery completion only from RECOVERY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [{ type: 'START_INTENT_DISCOVERY' }],
  },

  // ── Reconciliation drift — constitutional equilibrium compromised ──────
  RECONCILIATION_DRIFT_DETECTED: {
    target: 'DEGRADED',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY') {
        return { allowed: false, reason: `Reconciliation drift escalation only from HEALTHY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'RECONCILING',
      reason: event.reason || 'Reconciliation drift detected — constitutional equilibrium compromised',
    }],
  },

  RECONCILIATION_CLEARED: {
    target: 'HEALTHY',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED') {
        return { allowed: false, reason: `Reconciliation clear only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. General Guards — system-wide legality checks
// ═══════════════════════════════════════════════════════════════════════════════

function runGeneralGuards(currentState, targetState) {
  const results = [];

  // Guard: HALTED can only transition to BOOTING (manual restart)
  if (currentState === 'HALTED' && targetState !== 'BOOTING') {
    results.push({
      name: 'halted_lockdown',
      passed: false,
      reason: `HALTED state only allows transition to BOOTING, not ${targetState}`,
    });
    return results;
  }

  // Guard: RECOVERY blocks all transitions except to HEALTHY, HALTED, or BOOTING
  if (currentState === 'RECOVERY' &&
      targetState !== 'HEALTHY' &&
      targetState !== 'HALTED' &&
      targetState !== 'BOOTING') {
    results.push({
      name: 'recovery_blocks_operational',
      passed: false,
      reason: `Cannot enter ${targetState} while recovering`,
    });
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Runtime state — module-level mutable state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _currentState = 'BOOTING';
let _stateEnteredAt = Date.now();
let _loopInterval = null;

let _accountIds = [];

// Domain registry
const _domains = new Map(); // domainName → fsm

// Rehydrated domain states — populated during rehydrate() from lineage
let _rehydratedDomainStates = null;

// Action subscription
const _actionSubscribers = new Map(); // actionType → Set<fn>
let _legacyActionSubscriber = null;

// Reconciliation drift aggregation — counters persist across epochs
const _driftCounters = { substrate: 0, replay: 0 };
const DRIFT_ESCALATION_THRESHOLD = 3; // consecutive epochs before escalation

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Action dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Emit an observability transition for constitutional-layer events.
 * Centralizes the fire-and-forget observability call used across dispatch paths.
 * Gap 4 fix: All dispatch outcomes (including blocked/null-target) now emit
 * observability transitions so the lineage worker has a complete consumption feed.
 *
 * @param {string} from — prior global state
 * @param {string} to — resultant (or same if blocked/no-change)
 * @param {object} details — intent, reason, legitimacy context
 */
function _emitGovernanceTransition(from, to, details = {}) {
  try {
    const obs = _getObservabilityTransition();
    if (obs) {
      obs.transition({
        domain: 'governance',
        entity: 'runtime',
        entityId: 'global',
        previousState: from,
        nextState: to,
        authority: 'constitutional-kernel',
        raw: {
          intent: details.intent || null,
          substate: details.substate || null,
          reason: details.reason || null,
          blocked: details.blocked || false,
          epochId: details.epochId || null,
        },
      });
    }
  } catch (_) {}
}

function _emitActions(actions) {
  if (!actions || actions.length === 0) return;
  for (const action of actions) {
    // ── Kernel-internal actions ──────────────────────────────────────────
    if (action.type === 'UPDATE_ACCOUNTS') {
      _accountIds = Array.isArray(action.accountIds) ? action.accountIds : [];
      continue;
    }

    // ── Route to per-action-type subscribers ─────────────────────────────
    const subscribers = _actionSubscribers.get(action.type);
    if (subscribers && subscribers.size > 0) {
      for (const fn of subscribers) {
        try { fn(action); } catch (err) {
          console.error(`[constitutional-kernel] Subscriber error for ${action.type}:`, err.message);
        }
      }
    }

    // ── Legacy catch-all subscriber ──────────────────────────────────────
    if (_legacyActionSubscriber) {
      try { _legacyActionSubscriber(action); } catch (err) {
        console.error(`[constitutional-kernel] Legacy subscriber error for ${action.type}:`, err.message);
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Domain transition validation — called by domain FSMs before committing
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Validates a domain FSM's proposed local transition against global invariants.
 * Called by domain FSMs via ctx.validate() before they commit any state change.
 *
 * @param {string} domainName — domain requesting the transition
 * @param {string} from — domain-local prior state
 * @param {string} to — domain-local proposed target state
 * @param {object} event — the governance event triggering the transition
 * @returns {{ allowed: boolean, reason?: string }}
 */
function validateDomainTransition(domainName, from, to, event) {
  // HALTED lockdown — no domain transitions allowed
  if (_currentState === 'HALTED') {
    return { allowed: false, reason: 'Global runtime is HALTED — all domain transitions blocked' };
  }

  // DEGRADED restriction — allow scheduling (health checks) but block others
  if (_currentState === 'DEGRADED' && domainName !== 'scheduling') {
    return { allowed: false, reason: `Domain transitions blocked while global is DEGRADED` };
  }

  // RECOVERY restriction — allow scheduling (health checks) but block others
  if (_currentState === 'RECOVERY' && domainName !== 'scheduling') {
    return { allowed: false, reason: `Domain transitions blocked during RECOVERY` };
  }

  return { allowed: true };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Domain registration
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Register a domain FSM with the constitutional kernel.
 * Initializes the FSM with rehydrated state if lineage was loaded from Redis.
 *
 * @param {object} fsm — domain FSM module
 * @throws {Error} if fsm is invalid
 */
function registerDomain(fsm) {
  if (!fsm || typeof fsm !== 'object' || typeof fsm.name !== 'string' || typeof fsm.dispatch !== 'function') {
    throw new Error('[constitutional-kernel] registerDomain requires a valid domain FSM');
  }
  _domains.set(fsm.name, fsm);

  // Initialize domain FSM with rehydrated state from lineage (if available)
  if (_rehydratedDomainStates && typeof fsm.init === 'function') {
    const rehydratedState = _rehydratedDomainStates[fsm.name];
    if (rehydratedState) {
      fsm.init(rehydratedState);
      console.log(`[constitutional-kernel] Domain '${fsm.name}' initialized with rehydrated state: ${rehydratedState}`);
    }
  }

  console.log(`[constitutional-kernel] Registered domain: ${fsm.name}`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Dispatch — single entry point for ALL governance events
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Dispatch a runtime event into governance.
 * Routes to the appropriate domain FSM or handles as a global constitutional event.
 *
 * Write order invariant (Lineage-First):
 *   1. Lineage record (commit to canonical ledger)
 *   2. State materialization (mutate runtime state)
 *
 * @param {{ type: string, [key: string]: any }} event
 * @returns {{ allowed: boolean, from?: string, to?: string, lineageId?: string, actionsEmitted?: number, reason?: string }}
 */
function dispatch(event) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  // ── Route to domain FSM ────────────────────────────────────────────────
  const domainName = DOMAIN_EVENT_MAP[event.type];
  if (domainName) {
    const fsm = _domains.get(domainName);
    if (!fsm) {
      return { allowed: false, reason: `domain '${domainName}' not registered for event ${event.type}` };
    }

    const ctx = {
      validate: (from, to, evt) => validateDomainTransition(domainName, from, to, evt),
      dispatchGlobal: (globalEvent) => dispatch(globalEvent),
      getGlobalState: () => _currentState,
    };

    const result = fsm.dispatch(event, ctx);

    if (result.allowed && result.actions && result.actions.length > 0) {
      _emitActions(result.actions);
    }

    // Domain event lineage is written by the lineage worker (consuming from observability plane).
    // CK no longer appends domain events directly to the lineage ledger.
    // Constitutional layer events (global state transitions) are still written by CK.

    return {
      allowed: result.allowed,
      from: result.from || null,
      to: result.to || null,
      lineageId: result.lineageId || null,
      actionsEmitted: result.allowed && result.actions ? result.actions.length : 0,
      reason: result.reason || null,
    };
  }

  // ── Handle as global constitutional event ──────────────────────────────
  const txn = GLOBAL_TRANSITION_MAP[event.type];

  // ── Kernel-internal: RECONCILIATION_TICK triggers the reconciliation cycle ─
  if (event.type === 'RECONCILIATION_TICK') {
    return _onReconciliationTick();
  }

  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _currentState;
  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event, { state: from }) : rawTarget;

  // null target = no state change
  if (target === null) {
    _emitGovernanceTransition(from, from, { intent: event.type, reason: 'no-transition: event recorded' });
    return { allowed: true, from, to: from, actionsEmitted: 0, reason: 'no-transition: event recorded' };
  }

  // Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event, { state: from });
    if (!result.allowed) {
      _emitGovernanceTransition(from, from, {
        intent: event.type,
        reason: result.reason || 'guard blocked',
        blocked: true,
      });
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // Run general guards
  const generalResults = runGeneralGuards(from, target);
  const failedGeneral = generalResults.find(g => !g.passed);
  if (failedGeneral) {
    _emitGovernanceTransition(from, from, {
      intent: event.type,
      reason: failedGeneral.reason,
      blocked: true,
    });
    return { allowed: false, reason: failedGeneral.reason };
  }

  // Materialize state
  _currentState = target;
  _stateEnteredAt = Date.now();

  // Emit observability transition for global runtime state change
  _emitGovernanceTransition(from, target, {
    intent: event.type,
    substate: event.substate || null,
    reason: event.reason || null,
  });

  // Build actions
  const actions = txn.buildActions ? txn.buildActions(event, { state: from }) : [];

  // Emit actions
  _emitActions(actions);

  console.log(`[constitutional-kernel] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    actionsEmitted: actions.length,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 9. Action subscription
// ═══════════════════════════════════════════════════════════════════════════════

function subscribeAction(actionType, fn) {
  if (typeof actionType !== 'string' || !actionType) {
    throw new Error(`[constitutional-kernel] subscribeAction requires a non-empty actionType string`);
  }
  if (typeof fn !== 'function') {
    throw new Error(`[constitutional-kernel] subscribeAction handler must be a function, got ${typeof fn}`);
  }
  if (!_actionSubscribers.has(actionType)) {
    _actionSubscribers.set(actionType, new Set());
  }
  _actionSubscribers.get(actionType).add(fn);
}

function onAction(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[constitutional-kernel] onAction handler must be a function, got ${typeof fn}`);
  }
  _legacyActionSubscriber = fn;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 10. Watchdog — state staleness detection
// ═══════════════════════════════════════════════════════════════════════════════

function tick() {
  const elapsed = Date.now() - _stateEnteredAt;
  const ttl = STATE_TTL_MS[_currentState];
  if (ttl == null || elapsed < ttl) return;

  const reason = `State TTL exceeded: ${_currentState} for ${elapsed}ms (limit ${ttl}ms)`;

  switch (_currentState) {
    case 'BOOTING':
      dispatch({ type: 'FATAL_ERROR', reason });
      break;
    case 'DEGRADED':
      dispatch({ type: 'RECOVERY_INITIATED', reason });
      break;
    case 'RECOVERY':
      dispatch({ type: 'RECOVERY_COMPLETE' });
      break;
    // HEALTHY and HALTED have Infinity TTL — never auto-transition
  }
}

function startLoop(intervalMs = 10_000) {
  if (typeof intervalMs !== 'number' || intervalMs < 1000) {
    throw new Error(`[constitutional-kernel] intervalMs must be >= 1000, got ${intervalMs}`);
  }
  if (_loopInterval) {
    console.warn('[constitutional-kernel] Watchdog loop already running — ignoring duplicate start');
    return;
  }
  _loopInterval = setInterval(tick, intervalMs);
  _loopInterval.unref();
  console.log(`[constitutional-kernel] Watchdog loop started — tick every ${intervalMs}ms`);
}

function stopLoop() {
  if (_loopInterval) {
    clearInterval(_loopInterval);
    _loopInterval = null;
    console.log('[constitutional-kernel] Watchdog loop stopped');
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11. Reconciliation — constitutional equilibrium verification
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Build the substrate query interface for the reconciliation engine.
 * Provides bounded access to operational substrates for three-reality comparison.
 */
function _buildSubstrateQueries() {
  const dedupSubstrate = require('../../substrates/dedup-substrate');
  const retrySubstrate = require('../../substrates/retry');
  const metricsSubstrate = require('../../substrates/metrics-substrate');
  const cadence = require('../runtime/cadence');

  return {
    dedupIsInFlight: async (accountId, actionType, resourceId) => {
      return dedupSubstrate.isInFlight(accountId, actionType, resourceId);
    },
    retryInFlight: (accountId) => {
      return retrySubstrate.isAccountRateLimited ? retrySubstrate.isAccountRateLimited(accountId) : false;
    },
    bufferSnapshot: () => {
      const buffer = require('../runtime/buffer');
      try {
        return buffer.snapshot ? buffer.snapshot() : { size: 0, flushing: false };
      } catch {
        return { size: 0, flushing: false };
      }
    },
    metricsSignals: () => {
      return metricsSubstrate.getHealthSignals ? metricsSubstrate.getHealthSignals() : {};
    },
    cadenceLastTick: () => {
      return cadence.lastTick ? cadence.lastTick() : null;
    },
  };
}

/**
 * Run a reconciliation cycle by calling the standalone reconciliation engine.
 * The engine computes drift signals, classifies severity, and returns results.
 * The CK then verifies the result and decides corrective action.
 *
 * @returns {{ allowed: boolean, lineageId?: string, reason?: string }}
 */
async function _onReconciliationTick() {
  console.log('[constitutional-kernel] RECONCILIATION_TICK — running reconciliation cycle');

  try {
    const substrates = _buildSubstrateQueries();
    const result = await reconciliationEngine.runCycle({
      fsms: _domains,
      substrates,
      lineageLedger,
    });

    const { epochId, hash, observations, worstSeverity } = result;

    // ── Verify constitutional hash ────────────────────────────────────────
    const currentHash = await lineageLedger.computeHash();
    if (hash !== currentHash) {
      console.error(`[constitutional-kernel] Constitutional HASH MISMATCH — epoch ${epochId.slice(0, 8)}`);
      _resetDriftCounters();
      _emitGovernanceTransition(_currentState, _currentState, {
        intent: 'RECONCILIATION_HASH_MISMATCH',
        reason: `Constitutional identity divergence — epoch ${epochId.slice(0, 8)}`,
        epochId,
      });
      return { allowed: true, reason: 'hash mismatch detected — constitutional identity divergence' };
    }

    // ── Update drift counters ─────────────────────────────────────────────
    if (worstSeverity === reconciliationEngine.DRIFT_SEVERITY.NONE) {
      _resetDriftCounters();
      console.log(`[constitutional-kernel] Reconciliation epoch ${epochId.slice(0, 8)}: CONVERGENT — no drift detected`);
    } else {
      // Increment relevant counters
      for (const obs of observations) {
        if (obs.severity === reconciliationEngine.DRIFT_SEVERITY.SUBSTRATE) {
          _driftCounters.substrate++;
        } else if (obs.severity === reconciliationEngine.DRIFT_SEVERITY.REPLAY) {
          _driftCounters.replay++;
        }
      }

      const driftedDomains = observations.filter(o => o.severity > 0).map(o => o.domain).join(', ');
      console.warn(
        `[constitutional-kernel] Reconciliation epoch ${epochId.slice(0, 8)}: DRIFTED (severity=${worstSeverity}, domains=[${driftedDomains}], counters={substrate:${_driftCounters.substrate}, replay:${_driftCounters.replay}})`
      );
    }

    // ── Escalation: substrate drift persists across threshold ─────────────
    const multiDomainDrift = observations.filter(o => o.severity >= reconciliationEngine.DRIFT_SEVERITY.SUBSTRATE).length >= 2;

    if (_driftCounters.substrate >= DRIFT_ESCALATION_THRESHOLD || multiDomainDrift) {
      console.error(`[constitutional-kernel] Reconciliation drift escalated — substrate counter ${_driftCounters.substrate}/${DRIFT_ESCALATION_THRESHOLD}, multiDomain=${multiDomainDrift}`);
      _resetDriftCounters();

      // Dispatch into global transition map — enters DEGRADED with RECONCILING substate
      dispatch({
        type: 'RECONCILIATION_DRIFT_DETECTED',
        reason: multiDomainDrift
          ? `Multi-domain substrate drift detected: ${observations.filter(o => o.severity >= 3).map(o => o.domain).join(', ')}`
          : `Substrate drift persisted across ${DRIFT_ESCALATION_THRESHOLD} consecutive epochs`,
        details: { epochId, observations: observations.filter(o => o.severity >= 3) },
      });

      return { allowed: true, reason: 'reconciliation drift escalated to DEGRADED' };
    }

    return { allowed: true, reason: 'reconciliation cycle complete' };
  } catch (err) {
    console.error('[constitutional-kernel] Reconciliation cycle failed:', err.message);
    return { allowed: false, reason: `reconciliation cycle error: ${err.message}` };
  }
}

function _resetDriftCounters() {
  _driftCounters.substrate = 0;
  _driftCounters.replay = 0;
}

/**
 * Trigger a reconciliation cycle externally.
 * Called by the orchestrator on a 60s timer, independent of maintenance cadence.
 */
function triggerReconciliation() {
  dispatch({ type: 'RECONCILIATION_TICK' });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 12. Rehydration — load lineage from Redis and reconstruct state on boot
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Rehydrate the constitutional kernel from persisted lineage in Redis.
 * Loads all lineage entries, materializes global and domain states,
 * and stores rehydrated domain states for FSM initialization.
 *
 * Called automatically at module boot time. Safe to call multiple times.
 *
 * @returns {Promise<{ loaded: number, globalState: string, domains: object, latestTs: number|null }>}
 */
async function rehydrate() {
  try {
    const { loaded, latestTs } = await lineageLedger.rehydrate();
    if (loaded === 0) {
      console.log('[constitutional-kernel] Rehydration: empty lineage, starting fresh');
      return { loaded: 0, globalState: 'BOOTING', domains: {}, latestTs: null };
    }

    const entries = await lineageLedger.getLineage();
    const materialized = lineageLedger.materializeState(entries);

    _currentState = materialized.globalState;
    _stateEnteredAt = materialized.lastEvent ? materialized.lastEvent.ts : Date.now();
    _rehydratedDomainStates = materialized.domains;

    console.log(`[constitutional-kernel] Rehydration: ${loaded} entries, globalState='${_currentState}', domains=${JSON.stringify(materialized.domains)}`);

    return {
      loaded,
      globalState: _currentState,
      domains: materialized.domains,
      latestTs,
    };
  } catch (err) {
    console.error('[constitutional-kernel] Rehydration failed:', err.message);
    // Fast fail — do not boot with stale/default state if lineage cannot be loaded
    throw err;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 13. Observability
// ═══════════════════════════════════════════════════════════════════════════════

async function status() {
  const now = Date.now();
  const domainStates = {};
  for (const [name, fsm] of _domains) {
    domainStates[name] = fsm.exportState ? fsm.exportState() : { state: fsm.getState ? fsm.getState() : 'unknown' };
  }

  return {
    state: _currentState,
    lineageSize: await lineageLedger.getSize(),
    uptimeMs: now - STARTED_AT,
    stateDurationMs: now - _stateEnteredAt,
    domains: domainStates,
    accountIds: _accountIds.length,
  };
}

function getState() {
  return _currentState;
}

async function getLineage(n) {
  return lineageLedger.getLineage(n);
}

function getAccountIds() {
  return _accountIds;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 14. Domain FSM state query proxies — delegate to acquisition domain
// ═══════════════════════════════════════════════════════════════════════════════

function isCircuitBreakerActive(accountId) {
  const fsm = _domains.get('acquisition');
  return fsm && typeof fsm.isCircuitBreakerActive === 'function' ? fsm.isCircuitBreakerActive(accountId) : false;
}

function getAuthStrikes(accountId) {
  const fsm = _domains.get('acquisition');
  return fsm && typeof fsm.getAuthStrikes === 'function' ? fsm.getAuthStrikes(accountId) : 0;
}

function getRetryCount(intentId) {
  const fsm = _domains.get('acquisition');
  return fsm && typeof fsm.getRetryCount === 'function' ? fsm.getRetryCount(intentId) : 0;
}

function resetAuthStrikes(accountId) {
  const fsm = _domains.get('acquisition');
  if (fsm && typeof fsm.resetAuthStrikes === 'function') fsm.resetAuthStrikes(accountId);
}

function clearCircuitBreaker(accountId) {
  const fsm = _domains.get('acquisition');
  if (fsm && typeof fsm.clearCircuitBreaker === 'function') fsm.clearCircuitBreaker(accountId);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 15. Module initialization
// ═══════════════════════════════════════════════════════════════════════════════

// Rehydration is called explicitly by the orchestrator AFTER the lineage worker
// has started. This ensures CK reads from a ledger populated by the worker
// rather than rehydrating from an empty/potentially-stale Redis key.
// Boot order: orchestrator → observability.init() → worker.start() → CK.rehydrate()

module.exports = {
  dispatch,
  subscribeAction,
  onAction,
  registerDomain,
  validateDomainTransition,
  tick,
  startLoop,
  stopLoop,
  rehydrate,
  status,
  getState,
  getLineage,
  getAccountIds,
  isCircuitBreakerActive,
  getAuthStrikes,
  getRetryCount,
  resetAuthStrikes,
  clearCircuitBreaker,
  triggerReconciliation,
};
