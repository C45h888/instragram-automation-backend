// control-plane/governance/governance-kernel.js
// Governance Kernel: hierarchical state machine governing runtime legality.
//
// Owns: runtime state transitions, legality evaluation, lineage recording,
//        governance action emission.
// Does NOT own: Redis, Supabase, worker lifecycle, evaluation mechanics,
//               emission mechanics, buffering mechanics — those remain
//               implementation concerns of the runtime modules.
//
// Architecture invariant:
//   Runtime modules emit signals UPWARD   → governance.dispatch(event)
//   Governance emits authority DOWNWARD  → governance.onAction(action)
//   Orchestrator executes actions mechanically.
//
// Contract:
//   governance.dispatch(event)    → evaluate event, transition if legal, emit actions
//   governance.onAction(fn)       → register action subscriber (one subscriber max)
//   governance.tick()             → watchdog: checks state staleness, auto-recovers stuck states
//   governance.startLoop(ms)      → start internal watchdog interval (default 10s)
//   governance.stopLoop()         → stop internal watchdog interval
//   governance.status()           → { state, parentState, lineageSize, uptimeMs, stateDurationMs }
//   governance.getLineage([n])    → append-only lineage records
//   governance.getState()         → current full state string
//
// This is a PURE state machine — no I/O, no Redis, no Supabase, no side effects.
// The runtime is not "controlled" — it is GOVERNED. The governance kernel
// determines runtime meaning; the orchestrator executes runtime mechanics.

const crypto = require('crypto');

const STARTED_AT = Date.now();

// ═══════════════════════════════════════════════════════════════════════════════
// 1. State Registry — defines all legal runtime states and their hierarchy
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  // ── Parent states ──────────────────────────────────────────────────────────
  BOOTING: {
    parent: null,
    description: 'Runtime is initializing — modules, connections, and workers provisioning',
  },
  HEALTHY: {
    parent: null,
    description: 'Runtime is operating within normal parameters',
  },
  DEGRADED: {
    parent: null,
    description: 'Runtime is operating in a degraded capacity',
  },
  RECOVERY: {
    parent: null,
    description: 'Runtime is recovering from a degraded or halted state',
  },
  HALTED: {
    parent: null,
    description: 'Runtime has halted — manual intervention required',
  },

  // ── HEALTHY substates ──────────────────────────────────────────────────────
  'HEALTHY.IDLE': {
    parent: 'HEALTHY',
    description: 'Runtime is healthy but idle — no events currently processing',
  },
  'HEALTHY.BUFFERING': {
    parent: 'HEALTHY',
    description: 'Runtime is accumulating signal events in buffer',
  },
  'HEALTHY.EVALUATING': {
    parent: 'HEALTHY',
    description: 'Runtime is evaluating buffered events against publishing policy',
  },
  'HEALTHY.EMITTING': {
    parent: 'HEALTHY',
    description: 'Runtime is emitting publishing intents to Redis queues',
  },

  // ── DEGRADED substates ─────────────────────────────────────────────────────
  'DEGRADED.BACKPRESSURE': {
    parent: 'DEGRADED',
    description: 'Event buffer is backing up — buffer growing faster than evaluation',
  },
  'DEGRADED.RETRY_PRESSURE': {
    parent: 'DEGRADED',
    description: 'Worker retry rates are elevated — execution may be unstable',
  },
  'DEGRADED.PARTIAL_FAILURE': {
    parent: 'DEGRADED',
    description: 'One or more runtime modules have partially failed',
  },
  'DEGRADED.SIGNAL_DESYNC': {
    parent: 'DEGRADED',
    description: 'Signal intake is desynchronized — events may be lost',
  },

  // ── RECOVERY substates ─────────────────────────────────────────────────────
  'RECOVERY.RECONCILING': {
    parent: 'RECOVERY',
    description: 'Runtime is reconciling state between persistence and memory',
  },
  'RECOVERY.REHYDRATING': {
    parent: 'RECOVERY',
    description: 'Runtime is rehydrating state from persistence',
  },
  'RECOVERY.RESTARTING': {
    parent: 'RECOVERY',
    description: 'Runtime is restarting modules',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 1b. State TTL — maximum duration for each state before watchdog auto-recovers
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_TTL_MS = {
  BOOTING: 60_000,              // boot must complete within 60s
  'HEALTHY.IDLE': Infinity,     // idle can be indefinite
  'HEALTHY.BUFFERING': 30_000,  // buffer flush should happen quickly
  'HEALTHY.EVALUATING': 30_000, // evaluation should be fast
  'HEALTHY.EMITTING': 30_000,   // emission to Redis should be fast
  'DEGRADED.BACKPRESSURE': 120_000,
  'DEGRADED.RETRY_PRESSURE': 120_000,
  'DEGRADED.PARTIAL_FAILURE': 120_000,
  'DEGRADED.SIGNAL_DESYNC': 120_000,
  'RECOVERY.RECONCILING': 60_000,
  'RECOVERY.REHYDRATING': 60_000,
  'RECOVERY.RESTARTING': 60_000,
  HALTED: Infinity,             // manual intervention only
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Transition Map — event type → target state + guard + action builder
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Boot ───────────────────────────────────────────────────────────────────
  BOOT_COMPLETE: {
    target: 'HEALTHY.IDLE',
    buildActions: () => [],
  },

  // ── Operational flow: IDLE → BUFFERING → EVALUATING → EMITTING → IDLE ─────
  BUFFER_EVENT_INGESTED: {
    target: 'HEALTHY.BUFFERING',
    guard: (event, ctx) => {
      if (!['HEALTHY.IDLE', 'HEALTHY.BUFFERING'].includes(ctx.state)) {
        return { allowed: false, reason: `Cannot buffer from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  BUFFER_FLUSH_READY: {
    target: 'HEALTHY.EVALUATING',
    guard: (event, ctx) => {
      if (!['HEALTHY.IDLE', 'HEALTHY.BUFFERING'].includes(ctx.state)) {
        return { allowed: false, reason: `Cannot evaluate from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'EVALUATE',
      accountId: event.accountId,
      events: event.events,
    }],
  },

  EVALUATION_COMPLETE: {
    target: 'HEALTHY.EMITTING',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EVALUATING') {
        return { allowed: false, reason: `Cannot emit from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  EVALUATION_EMPTY: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EVALUATING') {
        return { allowed: false, reason: `Cannot go idle from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  EMISSION_COMPLETE: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EMITTING') {
        return { allowed: false, reason: `Cannot complete emission from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // ── HSM-governed acquisition: IDLE → EVALUATING → EMITTING → IDLE ─────────
  ACQUISITION_INTENT_RECEIVED: {
    target: 'HEALTHY.EVALUATING',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.IDLE') {
        return { allowed: false, reason: `Cannot acquire from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'EXECUTE_ACQUISITION',
      accountId: event.accountId,
      domain: event.domain,
      intentId: event.intentId,
      params: event.params,
    }],
  },

  ACQUISITION_EXECUTING: {
    target: 'HEALTHY.EMITTING',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EVALUATING') {
        return { allowed: false, reason: `Cannot execute from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  ACQUISITION_COMPLETE: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EMITTING') {
        return { allowed: false, reason: `Cannot complete from ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => (event.result ? [{
      type: 'WRITE_ACQUISITION_RESULT',
      accountId: event.accountId,
      domain: event.domain,
      intentId: event.intentId,
      result: event.result,
    }] : []),
  },

  // ── Degradation ────────────────────────────────────────────────────────────
  BACKPRESSURE_DETECTED: {
    target: 'DEGRADED.BACKPRESSURE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY')) {
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
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (ctx.state !== 'DEGRADED.BACKPRESSURE') {
        return { allowed: false, reason: `Backpressure not active (currently ${ctx.state})` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // Generic pressure clear — any DEGRADED substate → HEALTHY.IDLE.
  // Used by the cadence health check when worker metrics return to normal.
  PRESSURE_CLEARED: {
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED')) {
        return { allowed: false, reason: `Pressure clear only from DEGRADED, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  EMISSION_FAILED: {
    target: 'DEGRADED.PARTIAL_FAILURE',
    guard: (event, ctx) => {
      if (ctx.state !== 'HEALTHY.EMITTING' && ctx.state !== 'HEALTHY.EVALUATING') {
        return { allowed: false, reason: `Emission failure only from EMITTING/EVALUATING, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'PARTIAL_FAILURE',
      reason: event.reason || 'Emission to Redis failed',
    }],
  },

  // ── Recovery ───────────────────────────────────────────────────────────────
  RECOVERY_INITIATED: {
    target: 'RECOVERY.RECONCILING',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED')) {
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
    target: 'HEALTHY.IDLE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'RECOVERY')) {
        return { allowed: false, reason: `Recovery completion only from RECOVERY, got ${ctx.state}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // ── Halt ───────────────────────────────────────────────────────────────────
  FATAL_ERROR: {
    target: 'HALTED',
    buildActions: (event) => [{
      type: 'LOG_HALT',
      reason: event.reason || 'Unspecified fatal error',
    }],
  },

  // ── Substate transitions (within DEGRADED / RECOVERY) ─────────────────────
  RETRY_PRESSURE_DETECTED: {
    target: 'DEGRADED.RETRY_PRESSURE',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'DEGRADED') && !isDescendantOf(ctx.state, 'HEALTHY')) {
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
    target: 'DEGRADED.SIGNAL_DESYNC',
    guard: (event, ctx) => {
      if (!isDescendantOf(ctx.state, 'HEALTHY') && !isDescendantOf(ctx.state, 'DEGRADED')) {
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
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. General Guards — system-wide legality checks evaluated before every transition
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Run all general-guard checks against a proposed transition.
 * Returns an array of guard results. If any guard fails, the transition
 * is blocked regardless of per-transition guard results.
 */
function runGeneralGuards(currentState, targetState) {
  const results = [];

  // Guard: HALTED can only transition to BOOTING (manual restart)
  if (currentState === 'HALTED' && targetState !== 'BOOTING') {
    results.push({
      name: 'halted_lockdown',
      passed: false,
      reason: `HALTED state only allows transition to BOOTING, not ${targetState}`,
    });
    return results; // terminal — no need to check other guards
  }

  // Guard: DEGRADED parent blocks HEALTHY operational substates
  // Exception: DEGRADED → HEALTHY.IDLE (degradation cleared via backpressure_cleared)
  if (isDescendantOf(currentState, 'DEGRADED') &&
      isDescendantOf(targetState, 'HEALTHY') &&
      targetState !== 'HEALTHY.IDLE' &&
      !isDescendantOf(targetState, 'RECOVERY') &&
      targetState !== 'HALTED') {
    results.push({
      name: 'degraded_blocks_operational',
      passed: false,
      reason: `Cannot enter ${targetState} while in degraded state ${currentState}`,
    });
  }

  // Guard: RECOVERY parent blocks all transitions except to HEALTHY.IDLE, other RECOVERY substates, or HALTED
  if (isDescendantOf(currentState, 'RECOVERY') &&
      targetState !== 'HEALTHY.IDLE' &&
      !isDescendantOf(targetState, 'RECOVERY') &&
      targetState !== 'HALTED' &&
      targetState !== 'BOOTING') {
    results.push({
      name: 'recovery_blocks_operational',
      passed: false,
      reason: `Cannot enter ${targetState} while recovering (${currentState})`,
    });
  }

  // Guard: Cannot enter EMITTING from any DEGRADED state
  if (targetState === 'HEALTHY.EMITTING' && isDescendantOf(currentState, 'DEGRADED')) {
    results.push({
      name: 'no_emit_while_degraded',
      passed: false,
      reason: `Cannot emit while in degraded state ${currentState}`,
    });
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. State helpers
// ═══════════════════════════════════════════════════════════════════════════════

function parentOf(state) {
  const entry = STATE_REGISTRY[state];
  return entry ? entry.parent : null;
}

function isDescendantOf(state, ancestor) {
  let current = state;
  while (current) {
    if (current === ancestor) return true;
    current = parentOf(current);
  }
  return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Runtime state — module-level mutable state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _currentState = 'BOOTING';
let _stateEnteredAt = Date.now(); // when we entered _currentState — watchdog uses this
const _lineage = [];              // append-only
let _actionSubscriber = null;
let _loopInterval = null;         // watchdog setInterval handle

// Redis injection — set by orchestrator after governance startup.
// Used by tick() to discover acquisition intents from domain queues.
let _redis = null;
let _activeAccountIds = [];       // account UUIDs for queue polling
let _lastPolledDomainIdx = 0;     // round-robin index across domains per tick

const ACQUISITION_DOMAINS = ['comments','messages','ugc','insights','media'];
const ACQUISITION_PUBLISH_DOMAINS = ['media','ugc','messaging'];

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Lineage recorder
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Record a transition in the append-only lineage log.
 * Never mutated or deleted after append.
 */
function _recordLineage(from, to, trigger, guardResults, actions, meta) {
  const entry = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    from,
    to,
    trigger,
    guardResults: guardResults.map(g => ({
      name: g.name,
      passed: g.passed,
      reason: g.reason || null,
    })),
    actions: actions.map(a => ({ type: a.type })),
    meta,
  };
  _lineage.push(entry);
  return entry;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Action dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Emit governance actions after a successful transition.
 * Actions are routing intents — they describe WHAT should happen, not HOW.
 * The orchestrator subscriber executes them mechanically.
 */
function _emitActions(actions) {
  if (!_actionSubscriber || actions.length === 0) return;
  for (const action of actions) {
    try {
      _actionSubscriber(action);
    } catch (err) {
      console.error(`[governance-kernel] Action subscriber error for ${action.type}:`, err.message);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7b. Watchdog tick — state staleness detection and auto-recovery
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Watchdog tick — checks if the current state has exceeded its TTL and
 * triggers automatic recovery transitions. Also discovers acquisition
 * intents from Redis domain queues when the runtime is idle.
 *
 * Called by the internal loop interval. This is how the governance kernel
 * self-corrects when external events stop arriving AND how it discovers
 * new acquisition intents from the agent-reachable Redis queues.
 *
 * Dispatches events through the normal dispatch() path, so all transitions
 * are guarded, lineage is recorded, and actions are emitted.
 */
function tick() {
  // ── Intent discovery: poll Redis queues when idle ──────────────────────
  if (_currentState === 'HEALTHY.IDLE' && _redis && _activeAccountIds.length > 0) {
    _discoverIntents();
  }

  // ── Staleness detection ────────────────────────────────────────────────
  const elapsed = Date.now() - _stateEnteredAt;
  const ttl = STATE_TTL_MS[_currentState];
  if (ttl == null || elapsed < ttl) return;

  const reason = `State TTL exceeded: ${_currentState} for ${elapsed}ms (limit ${ttl}ms)`;

  switch (_currentState) {
    case 'BOOTING':
      dispatch({ type: 'FATAL_ERROR', reason });
      break;
    case 'HEALTHY.BUFFERING':
      dispatch({ type: 'BACKPRESSURE_DETECTED', reason });
      break;
    case 'HEALTHY.EVALUATING':
    case 'HEALTHY.EMITTING':
      dispatch({ type: 'EMISSION_FAILED', reason });
      break;
    case 'DEGRADED.BACKPRESSURE':
    case 'DEGRADED.RETRY_PRESSURE':
    case 'DEGRADED.PARTIAL_FAILURE':
    case 'DEGRADED.SIGNAL_DESYNC':
      dispatch({ type: 'RECOVERY_INITIATED', reason });
      break;
    case 'RECOVERY.RECONCILING':
    case 'RECOVERY.REHYDRATING':
    case 'RECOVERY.RESTARTING':
      dispatch({ type: 'RECOVERY_COMPLETE' });
      break;
    // HEALTHY.IDLE and HALTED have Infinity TTL — never auto-transition
  }
}

// ── Intent discovery: round-robin poll across domain queues per account ─────

/**
 * Polls Redis domain queues for acquisition intents.
 * Round-robin: one domain per account per tick to avoid queue starvation.
 * Each tick discovers at most one intent — deterministic pacing.
 *
 * Called by tick() only when _currentState === 'HEALTHY.IDLE'.
 */
function _discoverIntents() {
  const allDomains = [...ACQUISITION_DOMAINS, ...ACQUISITION_PUBLISH_DOMAINS.map(d => `publish:${d}`)];
  const domain = allDomains[_lastPolledDomainIdx % allDomains.length];
  _lastPolledDomainIdx = (_lastPolledDomainIdx + 1) % allDomains.length;

  // Pick one account per tick (rotates across accounts across ticks)
  const accountIdx = Math.floor(_lastPolledDomainIdx / allDomains.length) % _activeAccountIds.length;
  const accountId = _activeAccountIds[accountIdx];

  if (!accountId) return;

  const queueKey = `supervisor:acquisitions:${domain}:${accountId}`;

  // Non-blocking poll — if queue is empty, skip this tick
  _redis.lpop(queueKey).then(raw => {
    if (!raw) return;

    let intent;
    try { intent = JSON.parse(raw); } catch { return; }

    if (!intent || !intent.intent_id) return;

    dispatch({
      type: 'ACQUISITION_INTENT_RECEIVED',
      accountId,
      domain,
      intentId: intent.intent_id,
      params: intent.payload || intent.parameters || {},
    });
  }).catch(err => {
    // Redis error during poll — log, don't crash the watchdog
    console.error(`[governance-kernel] Redis poll error (${queueKey}):`, err.message);
  });
}

/**
 * Start the internal watchdog loop. Calls tick() every intervalMs.
 * Idempotent — safe to call on an already-running loop.
 * @param {number} [intervalMs=10000] — tick interval in ms, must be >= 1000
 * @throws {Error} if intervalMs < 1000
 */
function startLoop(intervalMs = 10_000) {
  if (typeof intervalMs !== 'number' || intervalMs < 1000) {
    throw new Error(`[governance-kernel] intervalMs must be >= 1000, got ${intervalMs}`);
  }
  if (_loopInterval) {
    console.warn('[governance-kernel] Watchdog loop already running — ignoring duplicate start');
    return;
  }
  _loopInterval = setInterval(tick, intervalMs);
  _loopInterval.unref();
  console.log(`[governance-kernel] Watchdog loop started — tick every ${intervalMs}ms`);
}

/**
 * Stop the internal watchdog loop.
 * Idempotent — safe to call when not running.
 */
function stopLoop() {
  if (_loopInterval) {
    clearInterval(_loopInterval);
    _loopInterval = null;
    console.log('[governance-kernel] Watchdog loop stopped');
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7c. Redis + account injection — required for intent discovery in tick()
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Inject Redis client for queue polling in tick().
 * Must be called before startLoop() for intent discovery to work.
 * @param {object} redis - ioredis client instance
 * @throws {Error} if redis is not an object
 */
function setRedis(redis) {
  if (!redis || typeof redis !== 'object') {
    throw new Error('[governance-kernel] setRedis requires a Redis client object');
  }
  _redis = redis;
}

/**
 * Update the active account IDs for round-robin queue polling.
 * Called by lifecycle.refresh() when accounts change.
 * @param {Array<string>} accountIds
 */
function setActiveAccounts(accountIds) {
  if (!Array.isArray(accountIds)) {
    throw new Error('[governance-kernel] setActiveAccounts requires an array');
  }
  _activeAccountIds = accountIds;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Public API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Dispatch a runtime event upward into the governance kernel.
 *
 * The event is evaluated against the transition map. If a matching transition
 * exists and all guards (per-transition + general) pass, the state transitions,
 * lineage is recorded, and governance actions are emitted.
 *
 * @param {{ type: string, [key: string]: any }} event — runtime signal event
 * @returns {{ allowed: boolean, from?: string, to?: string, lineageId?: string, actionsEmitted?: number, reason?: string }}
 *   Deterministic — same event in same state always produces the same result.
 */
function dispatch(event) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    // Unknown event — not an error, just not a state-changing event
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const target = txn.target;
  const from = _currentState;

  // 1. Run per-transition guard (if defined)
  let perGuardResults = [];
  if (txn.guard) {
    const result = txn.guard(event, { state: from });
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
    perGuardResults.push({
      name: `transition:${event.type}`,
      passed: true,
    });
  }

  // 2. Run general guards
  const generalResults = runGeneralGuards(from, target);
  const failedGeneral = generalResults.find(g => !g.passed);
  if (failedGeneral) {
    return { allowed: false, reason: failedGeneral.reason };
  }

  // 3. Execute transition
  _currentState = target;
  _stateEnteredAt = Date.now();
  const allGuardResults = [...perGuardResults, ...generalResults];

  // 4. Build actions
  const actions = txn.buildActions(event, { state: from });

  // 5. Record lineage
  const lineageEntry = _recordLineage(from, target, event.type, allGuardResults, actions, {
    accountId: event.accountId || null,
    eventCount: event.eventCount || null,
    intentCount: event.intentCount || null,
  });

  // 6. Emit actions
  _emitActions(actions);

  console.log(`[governance-kernel] ${from} → ${target}  (${event.type})  [${lineageEntry.id.slice(0, 8)}]`);

  return {
    allowed: true,
    from,
    to: target,
    lineageId: lineageEntry.id,
    actionsEmitted: actions.length,
  };
}

/**
 * Register the governance action subscriber. Only one subscriber is supported.
 * The subscriber receives governance actions emitted after successful transitions.
 * It is the orchestrator's responsibility to route actions to runtime modules.
 *
 * @param {Function} fn — (action: object) => void
 * @throws {Error} if fn is not a function
 */
function onAction(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[governance-kernel] onAction handler must be a function, got ${typeof fn}`);
  }
  _actionSubscriber = fn;
}

/**
 * Returns live governance state. Deterministic, no side effects.
 * @returns {{ state: string, parentState: string|null, lineageSize: number, uptimeMs: number }}
 */
function status() {
  return {
    state: _currentState,
    parentState: parentOf(_currentState),
    lineageSize: _lineage.length,
    uptimeMs: Date.now() - STARTED_AT,
    stateDurationMs: Date.now() - _stateEnteredAt,
  };
}

/**
 * Returns the last N lineage records (or all if n is omitted).
 * Records are append-only and never mutated.
 * @param {number} [n] — number of recent records to return
 * @returns {Array<object>}
 */
function getLineage(n) {
  if (typeof n === 'number' && n > 0) {
    return _lineage.slice(-n);
  }
  return [..._lineage];
}

/**
 * Returns the current full state string (e.g. 'HEALTHY.IDLE').
 * @returns {string}
 */
function getState() {
  return _currentState;
}

module.exports = { dispatch, onAction, tick, startLoop, stopLoop, setRedis, setActiveAccounts, status, getLineage, getState };
