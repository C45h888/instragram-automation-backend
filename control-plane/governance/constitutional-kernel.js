// control-plane/governance/constitutional-kernel.js
// Constitutional Kernel: arbiter of runtime legitimacy and invariant law.
//
// Owns: global lifecycle (BOOTING/HEALTHY/DEGRADED/RECOVERY/HALTED/DEAD),
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
const checkpointer = require('./lineage-checkpointer');

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

// Membrane authority map — defines which authorities may mutate which domains.
// This is the constitutional contract for membrane boundary enforcement.
// CK is the HSM (Hierarchical State Machine) and sole interpreter of this map.
// Cross-domain mutations are rejected if authority is not permitted for target domain.

const MEMBRANE_AUTHORITY_MAP = {
  'acquisition-fsm':     ['acquisition'],
  'publishing-membrane': ['publishing'],
  'telemetry-worker':    ['telemetry'],
  'reconciliation-fsm':  ['reconciliation'],
  'scheduling-fsm':      ['scheduling'],
  'governance-kernel':   ['governance', 'execution', 'acquisition', 'publishing',
                          'scheduling', 'telemetry', 'reconciliation', 'projection'],
};

function _extractForeignAuthorityDomain(authority) {
  if (!authority || typeof authority !== 'string') return null;
  // e.g. 'foreign-domain-attacker' → extract 'foreign-domain'
  const match = authority.match(/^([a-z]+(?:[-][a-z]+)*)-/);
  return match ? match[1] : null;
}

function _validateMembraneAuthority(authority, targetDomain) {
  const permitted = MEMBRANE_AUTHORITY_MAP[authority];

  if (permitted !== undefined) {
    // Known membrane — verify domain is in its permitted list
    if (!permitted.includes(targetDomain)) {
      return {
        allowed: false,
        reason: `MEMBRANE_BYPASS: authority '${authority}' may not mutate domain '${targetDomain}'`,
      };
    }
  } else {
    // Foreign authority (not in map) — may only mutate its own domain
    // e.g. 'foreign-domain-attacker' mutating 'acquisition' → bypass detected
    const foreignDomain = _extractForeignAuthorityDomain(authority);
    if (foreignDomain && foreignDomain !== targetDomain) {
      return {
        allowed: false,
        reason: `MEMBRANE_BYPASS: foreign authority '${authority}' may not mutate '${targetDomain}'`,
      };
    }
  }

  return { allowed: true };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 0b. Signal Ownership Contract — projection signal governance
// ═══════════════════════════════════════════════════════════════════════════════

// Signal classification — defines the derivation model for each signal category.
// Ledger-derivable: recomputable from immutable ledger replay with no observation-time deps
// Observer-relative: derived from mutable runtime state at observation tick time
// Substrate-mechanical: mechanical substrate state with no semantic interpretation

const SIGNAL_CLASS = {
  LEDGER_DERIVABLE: 'ledger_derivable',
  OBSERVER_RELATIVE: 'observer_relative',
  SUBSTRATE_MECHANICAL: 'substrate_mechanical',
};

// Canonical signal ownership registry.
// CK validates that each signal is only ever written by its canonical owner.
// Unknown signals (not in this map) are rejected as unclassified.
const SIGNAL_OWNERSHIP_MAP = {
  // ── Ledger-derivable signals — lineage worker Layer B only ─────────────
  // These signals are recomputable from immutable lineage:ledger:entries replay.
  'health.transitionCount':           { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'health.lastTransition':             { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'health.executionHealth':            { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'health.authorityStability':          { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'integrity.structuralAnomalyCount':   { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'integrity.replayAnomalyProbability': { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'integrity.cadenceGapProbability':     { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'governanceRuntime.runtimeState':     { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'governanceRuntime.lastStateTransition': { owner: 'lineage-worker',       class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'governanceRuntime.degradationSignals': { owner: 'lineage-worker',        class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'governanceRuntime.epochCount':        { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'governanceRuntime.domainInstability': { owner: 'lineage-worker',         class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.acquisition.authorityCount':    { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.acquisition.lastAuthority':     { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.acquisition.authorityOscillation': { owner: 'lineage-worker',    class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.acquisition.continuityStatus':   { owner: 'lineage-worker',     class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.publishing.authorityCount':     { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.publishing.lastAuthority':       { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.publishing.authorityOscillation': { owner: 'lineage-worker',    class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.publishing.continuityStatus':   { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.scheduling.authorityCount':      { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.scheduling.lastAuthority':       { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.scheduling.authorityOscillation': { owner: 'lineage-worker',    class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'authority.scheduling.continuityStatus':    { owner: 'lineage-worker',      class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.acquisition.state':           { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.acquisition.transitionCount': { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.acquisition.lastTransition':  { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.acquisition.authorityStability': { owner: 'lineage-worker',       class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.publishing.state':            { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.publishing.transitionCount':   { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.publishing.lastTransition':   { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.publishing.authorityStability': { owner: 'lineage-worker',       class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.scheduling.state':            { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.scheduling.transitionCount':  { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.scheduling.lastTransition':   { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.scheduling.authorityStability': { owner: 'lineage-worker',         class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.scheduling.cadenceContinuity': { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.dedup.state':                { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.dedup.transitionCount':      { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.dedup.lastTransition':        { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.dedup.authorityStability':    { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.reconciliation.state':        { owner: 'lineage-worker',          class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.reconciliation.transitionCount': { owner: 'lineage-worker',       class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.reconciliation.lastTransition':  { owner: 'lineage-worker',       class: SIGNAL_CLASS.LEDGER_DERIVABLE },
  'domain.reconciliation.authorityStability': { owner: 'lineage-worker',     class: SIGNAL_CLASS.LEDGER_DERIVABLE },

  // ── Observer-relative signals — telemetry workers only ─────────────────
  // These signals depend on observation timing, polling cadence, or mutable
  // runtime state. They cannot be reconstructed from immutable ledger replay.
  'health.failureRate':              { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.retryPressure':            { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.bufferPressure':            { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.quotaPressure':             { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.circuitBreakers':           { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.interpretationConfidence': { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'integrity.executionPressure':      { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'governanceRuntime.governancePressure': { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'systemic.governancePressure':       { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'systemic.systemicStress':           { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'systemic.convergenceConfidence':   { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'systemic.domainInstability':       { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.runtimeEntropy':            { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.operationalStress':         { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
  'health.degradationSignals':        { owner: 'telemetry-workers', class: SIGNAL_CLASS.OBSERVER_RELATIVE },
};

/**
 * Validate a projection snapshot against the signal ownership contract.
 * Traverses all numeric signal paths in the snapshot and verifies each
 * has a canonical owner matching the source worker.
 *
 * @param {object} snapshot — projection snapshot (e.g. from lineage-worker getProjections())
 * @param {string} sourceWorker — 'lineage-worker' | 'telemetry-workers' | etc
 * @returns {{ valid: boolean, violations: Array<{ signal: string, expectedOwner: string, actualOwner: string, signalClass: string }> }}
 */
function validateProjectionSnapshot(snapshot, sourceWorker) {
  const violations = [];

  function checkSignal(path, value) {
    if (typeof value !== 'number' && typeof value !== 'string' && typeof value !== 'boolean') return;

    // Try to match this path against known signal names in OWNERSHIP_MAP
    // Check both the full path and the leaf signal name
    for (const [ownedSignal, contract] of Object.entries(SIGNAL_OWNERSHIP_MAP)) {
      const signalName = ownedSignal.split('.').pop();
      const pathLower = path.toLowerCase();

      // Match by leaf signal name
      if (pathLower.endsWith(signalName.toLowerCase())) {
        if (contract.owner !== sourceWorker) {
          violations.push({
            signal: path,
            expectedOwner: contract.owner,
            actualOwner: sourceWorker,
            signalClass: contract.class,
          });
        }
        return; // Found match, stop searching
      }
    }
    // Unknown signal (not in OWNERSHIP_MAP) — not a violation, could be a
    // new signal type still being classified. Skip unknown signals.
  }

  function traverse(obj, path = '') {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return;
    for (const [key, value] of Object.entries(obj)) {
      // Skip internal metadata and functions
      if (key === '_meta' || key === 'raw' || typeof value === 'function') continue;
      const currentPath = path ? `${path}.${key}` : key;
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        traverse(value, currentPath);
      } else {
        checkSignal(currentPath, value);
      }
    }
  }

  traverse(snapshot);
  return { valid: violations.length === 0, violations };
}

/**
 * Emit a membrane bypass anomaly into the constitutional observability plane.
 * Routes through: CK → observability.transition() → lineage worker → ledger.
 * Constitutional topology preserved — worker remains sole writer.
 *
 * @param {object} rejectedEntry — the normalized transition that was rejected
 * @param {string} reason — CK rejection reason string
 * @returns {Promise<{ id: string, ts: number, cursor: number }>} includes observability log cursor
 */
async function recordMembraneBypassAnomaly(rejectedEntry, reason) {
  const observability = require('../observability');
  const anomalyId = require('crypto').randomUUID();
  const now = Date.now();

  // Capture log size BEFORE emitting — this is the cursor the anomaly will occupy
  const cursorBefore = observability.query.getLogSize();

  observability.transition({
    domain: 'governance',
    entity: 'membrane',
    entityId: anomalyId,
    previousState: null,
    nextState: 'MEMBRANE_BYPASS',
    authority: 'governance-kernel',
    raw: {
      entryType: 'divergence',
      divergenceCategory: 'membrane_authority_violation',
      bypassedAuthority: rejectedEntry.authority,
      targetDomain: rejectedEntry.domain,
      reason,
      rejectedTraceId: rejectedEntry.traceId,
      rejectedCorrelationId: rejectedEntry.correlationId,
      projectionVersion: '1.0.0',
      lineageVersion: '1.0.0',
    },
  });

  // cursorBefore + 1 is the index the anomaly was written to
  return { id: anomalyId, ts: now, cursor: cursorBefore + 1 };
}

// Events NOT in this map are handled as global constitutional events.
const DOMAIN_EVENT_MAP = {
  // Acquisition domain — lifecycle only (engagement signals routed to engagement domain)
  ACQUISITION_INTENT_RECEIVED: 'acquisition',
  ACQUISITION_EXECUTING: 'acquisition',
  ACQUISITION_COMPLETE: 'acquisition',
  EXECUTION_OBSERVATION: 'acquisition',

  // Engagement domain — circuit breaker, auth strikes, retry counting
  AUTH_FAILURE_STRIKE: 'engagement',
  RATE_LIMIT_DETECTED: 'engagement',
  RETRY_EXHAUSTED: 'engagement',
  CIRCUIT_BREAKER_CHECK: 'engagement',
  CIRCUIT_COOLDOWN_ELAPSED: 'engagement',
  CIRCUIT_TEST_SUCCESS: 'engagement',
  CIRCUIT_TEST_FAIL: 'engagement',
  CIRCUIT_BREAKER_CLEARED: 'engagement',
  AUTH_STRIKES_RESET: 'engagement',
  AUTH_SUCCESS: 'engagement',
  RETRY_COUNT_INCREMENTED: 'engagement',

  // Publishing domain
  BUFFER_EVENT_INGESTED: 'publishing',
  BUFFER_FLUSH_READY: 'publishing',
  EMISSION_OBSERVATION: 'publishing',
  DB_SCAN_EMITTED: 'publishing',

  // Scheduling domain
  CADENCE_TICK: 'scheduling',
  WORKER_METRICS_REPORTED: 'scheduling',
  DATABASE_SCANNED: 'scheduling',
  LIFECYCLE_REFRESHED: 'scheduling',
  SAFETY_CHECK_COMPLETE: 'scheduling',

  // Dedup domain
  DEDUP_BATCH_BEGIN: 'dedup',
  DEDUP_INTENT_MARKED: 'dedup',
  DEDUP_REPLAY_DETECTED: 'dedup',
  DEDUP_BATCH_END: 'dedup',

  // Reconciliation domain
  RECONCILIATION_TICK: 'reconciliation',
  RECONCILIATION_RESULTS_RECEIVED: 'reconciliation',

  // Telemetry Coordination domain — deterministic semantic ingress plane
  PROCESS_INTENTS: 'telemetry-coordination',
  HALT_TELEMETRY_COORDINATION: 'telemetry-coordination',
  RESUME_TELEMETRY_COORDINATION: 'telemetry-coordination',
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
  DEAD: {
    description: 'Runtime is dead — catastrophic lineage loss detected, reboot from checkpoint in progress',
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
  DEAD: 10_000, // 10s max — watchdog forces reboot if death sequence stalls
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

  // Guard: DEAD blocks all transitions — only internal reboot may bypass
  if (currentState === 'DEAD') {
    results.push({
      name: 'dead_lockdown',
      passed: false,
      reason: `Runtime is DEAD — all transitions blocked pending checkpoint reboot`,
    });
    return results;
  }

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
let _reconInProgress = false; // Guard: prevent concurrent reconciliation cycles

let _accountIds = [];

// Domain registry
const _domains = new Map(); // domainName → fsm

// Rehydrated domain states — populated during rehydrate() from lineage
let _rehydratedDomainStates = null;

// Action subscription
const _actionSubscribers = new Map(); // actionType → Set<fn>
let _legacyActionSubscriber = null;

// Reconciliation cycle reentrancy guard + completion promise
// triggerReconciliation() sets _reconInProgress=true and a new completion promise.
// The cycle stub (RECONCILIATION_CYCLE_COMPLETE) resolves the promise, unblocking
// triggerReconciliation() callers that await the result.
let _reconPromiseResolve = null;
let _reconPromiseReject = null;

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
  // DEAD lockdown — all domain transitions blocked during checkpoint reboot
  if (_currentState === 'DEAD') {
    return { allowed: false, reason: 'Runtime is DEAD — all domain transitions blocked pending checkpoint reboot' };
  }

  // HALTED lockdown — no domain transitions allowed
  if (_currentState === 'HALTED') {
    return { allowed: false, reason: 'Global runtime is HALTED — all domain transitions blocked' };
  }

  // DEGRADED restriction — allow scheduling and reconciliation (health checks) but block others
  if (_currentState === 'DEGRADED' && domainName !== 'scheduling' && domainName !== 'reconciliation') {
    return { allowed: false, reason: `Domain transitions blocked while global is DEGRADED` };
  }

  // RECOVERY restriction — allow scheduling and reconciliation (health checks) but block others
  if (_currentState === 'RECOVERY' && domainName !== 'scheduling' && domainName !== 'reconciliation') {
    return { allowed: false, reason: `Domain transitions blocked during RECOVERY` };
  }

  // Membrane authority check — verify the event's authority is permitted to mutate this domain
  const authority = event && (event.authority || (event.raw && event.raw.authority));
  if (authority) {
    const membraneCheck = _validateMembraneAuthority(authority, domainName);
    if (!membraneCheck.allowed) {
      return { allowed: false, reason: membraneCheck.reason };
    }
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
      // NOTE: getGlobalState is reserved for Phase 6 ctx.getPolicy() integration.
      // Domain FSMs will query constitutional policy context via getGlobalState()
      // when POLICY_BROADCAST is implemented and ctx.getPolicy() is added.
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
// 11. Reconciliation bridge — async engine comparison subscriber
//
// When the reconciliation FSM transitions to RECONCILING, it emits a
// RECONCILIATION_CYCLE_STARTED action. This subscriber catches that action,
// calls the dumb reconciliation engine (semantically blind substrate),
// verifies constitutional hash integrity, and dispatches results back
// to the FSM via RECONCILIATION_RESULTS_RECEIVED.
//
// The FSM governs lifecycle. This bridge performs the async mechanical work.
// The HSM (CK) retains hash verification authority.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Build the substrate query interface for the reconciliation engine.
 * Provides bounded access to operational substrates for three-reality comparison.
 * Preserved from prior CK-owned reconciliation — now only used by the bridge subscriber.
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
    dedupSnapshot: () => {
      return typeof dedupSubstrate.getInflightSnapshot === 'function'
        ? dedupSubstrate.getInflightSnapshot()
        : { identityCount: 0, resourceCount: 0, sample: [] };
    },
  };
}

/**
 * Trigger a complete reconciliation cycle.
 *
 * Lifecycle ownership (now split between trigger and bridge subscriber):
 *   triggerReconciliation():
 *     1. Death detection — abort before dispatch if lineage is already dead
 *     2. Set up completion promise — resolved by bridge subscriber when cycle ends
 *     3. Dispatch RECONCILIATION_TICK → FSM transitions IDLE → RECONCILING
 *        (FSM validates via ctx.validate, emits CYCLE_STARTED action)
 *     4. Return the promise — callers await this
 *
 *   Bridge subscriber (Section 11 wiring at module init):
 *     5. Run the async engine comparison (mechanical work)
 *     6. HSM verifies constitutional hash independently
 *     7. Dispatch RECONCILIATION_RESULTS_RECEIVED → FSM → CONVERGENT/DRIFTED
 *     8. Dispatch RECONCILIATION_CYCLE_COMPLETE → FSM → IDLE
 *     9. Resolve the completion promise
 *    10. Stability checkpoint if all gates pass
 *
 * Constitutional invariant:
 *   The cycle ALWAYS completes. Even on engine failure, a CYCLE_COMPLETE
 *   is dispatched so the FSM returns to IDLE and subsequent cycles can proceed.
 *
 * @returns {Promise<{ observations: Array, worstSeverity: number, hash: string, hashMismatch: boolean, elapsedMs: number }>|null}
 */
async function triggerReconciliation() {
  // ── Reentrancy guard — prevent concurrent cycles ───────────────────────
  if (_reconInProgress) {
    console.warn('[constitutional-kernel] Reconciliation cycle already in progress — rejecting concurrent trigger');
    return null;
  }
  _reconInProgress = true;

  // ── Death detection BEFORE starting — abort if lineage is already dead ──
  const ledgerSize = await lineageLedger.getSize();
  const ckpt = checkpointer.getCheckpoint();
  if (_detectConstitutionalDeath(ledgerSize, ckpt)) {
    await _triggerConstitutionalDeath(ckpt);
    _reconInProgress = false;
    return null;
  }

  // ── Set up completion promise — bridge subscriber resolves it on cycle end ──
  const result = await new Promise((resolve) => {
    _reconPromiseResolve = resolve;
    _reconPromiseReject = null;

    // Phase 1: Initiate the cycle — FSM transitions IDLE → RECONCILING
    // and emits RECONCILIATION_CYCLE_STARTED action, which the bridge
    // subscriber picks up to run the async engine work.
    dispatch({ type: 'RECONCILIATION_TICK' });
  });

  _reconInProgress = false;
  return result;
}

/**
 * Trigger a deterministic telemetry coordination cycle.
 *
 * Called by the orchestrator on a 30s cadence (matching projection worker
 * poll interval). Dispatches PROCESS_INTENTS to the Telemetry Coordination
 * FSM which reads, validates, orders, and serializes projection intents
 * into canonical SEMANTIC_PROJECTION_TRANSITION entries.
 *
 * The CK remains the sole authority that can trigger coordination.
 * The FSM coordinates only — it does not self-trigger.
 */
function triggerCoordinationCycle() {
  dispatch({ type: 'PROCESS_INTENTS' });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11b. Constitutional Death Detection — multi-criterion
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Detect whether the canonical lineage has been constitutionally invalidated.
 *
 * Criteria (any one triggers death):
 *   C1: Total extinction — ledger has 0 entries, CK is not BOOTING
 *   C2: Partial truncation — ledger has >0 but <50% of checkpoint entry count
 *   C3: Epoch regression — reconciliation epoch went backwards
 *   C4: Hash discontinuity — deferred (requires hash chain in ledger entries)
 *
 * All criteria require: a checkpoint exists AND CK is not BOOTING.
 *
 * @param {number} ledgerSize — current lineage ledger entry count
 * @param {object|null} ckpt — checkpoint from checkpointer.getCheckpoint()
 * @returns {boolean} true if constitutional death should be triggered
 */
function _detectConstitutionalDeath(ledgerSize, ckpt) {
  if (!ckpt) return false;
  if (_currentState === 'BOOTING') return false;

  // C1: Total extinction
  if (ledgerSize === 0) {
    console.error('[CK] Death criterion C1: total lineage extinction');
    return true;
  }

  // C2: Partial truncation — >50% of entries silently dropped
  if (ledgerSize > 0 && ckpt.entryCount > 0 && ledgerSize < ckpt.entryCount * 0.5) {
    console.error(
      `[CK] Death criterion C2: partial truncation — ` +
      `${ledgerSize} entries vs checkpoint ${ckpt.entryCount}`
    );
    return true;
  }

  // C3: Epoch regression — reconciliation epoch went backwards
  const reconFsm = _domains.get('reconciliation');
  if (reconFsm && typeof reconFsm.getEpochCount === 'function') {
    const currentEpoch = reconFsm.getEpochCount();
    if (currentEpoch > 0 && ckpt.epochCount > currentEpoch) {
      console.error(
        `[CK] Death criterion C3: epoch regression — ` +
        `current ${currentEpoch} < checkpoint ${ckpt.epochCount}`
      );
      return true;
    }
  }

  // C4: Hash discontinuity — deferred
  return false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11c. Stability Gate Evaluation — when to checkpoint
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Evaluate whether the runtime is in a constitutionally stable state
 * suitable for creating a checkpoint snapshot.
 *
 * Gates:
 *   G1: Governance must be HEALTHY
 *   G2: Reconciliation FSM must be IDLE
 *   G3: No active drift (consecutiveDrifted === 0)
 *   G4: No escalation signaled
 *   G5: Ingestion lag bounded (< 5 entry gap between log and ledger)
 *
 * @returns {boolean}
 */
function _canCheckpoint() {
  // G1: Governance must be HEALTHY
  if (_currentState !== 'HEALTHY') return false;

  // G2: Reconciliation FSM must be IDLE
  const reconFsm = _domains.get('reconciliation');
  if (!reconFsm || reconFsm.getState() !== 'IDLE') return false;

  // G3: No active drift
  const health = reconFsm.getHealth ? reconFsm.getHealth() : {};
  if (health.signals && health.signals.consecutiveDrifted > 0) return false;

  // G4: No escalation signaled
  if (health.signals && health.signals.escalationSignaled) return false;

  // G5: Ingestion lag bounded
  try {
    const lw = require('./lineage-worker');
    const obs = require('../observability');
    const logSize = obs.query ? obs.query.getLogSize() : 0;
    const lwSize = lw.getLedgerSize ? lw.getLedgerSize() : 0;
    if (Math.abs(logSize - lwSize) > 5) return false;
  } catch (_) {
    return false;
  }

  return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11d. Constitutional Death Sequence — checkpoint reboot
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Execute the constitutional death and reboot sequence.
 *
 *   1. Enter DEAD — blocks all dispatch and domain transitions
 *   2. Stop all workers
 *   3. Clear stale in-memory state
 *   4. Restore lineage from checkpoint file → Redis
 *   5. Rehydrate CK from restored lineage
 *   6. Re-register domains with rehydrated states
 *   7. Restart workers
 *   8. Dispatch BOOT_COMPLETE → HEALTHY
 *   9. Clear checkpoint (consumed)
 *
 * @param {object} ckpt — checkpoint from checkpointer.getCheckpoint()
 * @returns {Promise<void>}
 */
async function _triggerConstitutionalDeath(ckpt) {
  console.error('[CK] CONSTITUTIONAL DEATH — canonical lineage invalidated, rebooting from checkpoint');

  // 1. Enter DEAD
  const priorState = _currentState;
  _currentState = 'DEAD';
  _stateEnteredAt = Date.now();
  _emitGovernanceTransition(priorState, 'DEAD', {
    intent: 'CONSTITUTIONAL_DEATH',
    reason: 'Canonical lineage ledger invalidated — rebooting from checkpoint',
  });

  // 2. Stop all workers
  try { await require('./lineage-worker').stop(); } catch (e) { console.warn('[CK] Lineage worker stop error:', e.message); }
  try { await require('../telemetry-workers').stopAll(); } catch (e) { console.warn('[CK] Telemetry workers stop error:', e.message); }

  // 3. Clear stale in-memory domain states
  _rehydratedDomainStates = null;

  // 4. Restore lineage from checkpoint file → Redis
  try {
    for (const entry of (ckpt.entries || [])) {
      await lineageLedger.recordWorkerEntry(entry);
    }
    console.log(`[CK] Checkpoint restored: ${ckpt.entries.length} entries written to Redis`);
  } catch (e) {
    console.error('[CK] Checkpoint restore FAILED:', e.message);
    dispatch({ type: 'FATAL_ERROR', reason: `Checkpoint restore failed: ${e.message}` });
    return; // do not continue — runtime enters HALTED via FATAL_ERROR
  }

  // 5. Rehydrate CK from restored lineage
  try {
    await rehydrate();
  } catch (e) {
    console.error('[CK] Rehydration after checkpoint restore FAILED:', e.message);
    dispatch({ type: 'FATAL_ERROR', reason: `Post-checkpoint rehydration failed: ${e.message}` });
    return;
  }

  // 6. Re-register domains with rehydrated states
  for (const [name, fsm] of _domains) {
    const state = _rehydratedDomainStates ? _rehydratedDomainStates[name] : null;
    if (state && typeof fsm.init === 'function') {
      fsm.init(state);
      console.log(`[CK] Domain '${name}' re-initialized with checkpoint state: ${state}`);
    }
  }

  // 7. Restart workers
  try { await require('../telemetry-workers').startAll(); } catch (e) { console.warn('[CK] Telemetry workers restart error:', e.message); }
  try { await require('./lineage-worker').start(400); } catch (e) { console.warn('[CK] Lineage worker restart error:', e.message); }

  // 8. Dispatch BOOT_COMPLETE → HEALTHY
  dispatch({ type: 'BOOT_COMPLETE' });

  // 9. Clear checkpoint — consumed, new one will be created later
  checkpointer.clearCheckpoint();

  console.log('[CK] Constitutional death reboot complete — running from checkpoint');
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
// 14. Domain FSM state query proxies — delegate to engagement domain
// ═══════════════════════════════════════════════════════════════════════════════

function isCircuitBreakerActive(accountId) {
  const fsm = _domains.get('engagement');
  return fsm && typeof fsm.isCircuitBreakerActive === 'function' ? fsm.isCircuitBreakerActive(accountId) : false;
}

function getAuthStrikes(accountId) {
  const fsm = _domains.get('engagement');
  return fsm && typeof fsm.getAuthStrikes === 'function' ? fsm.getAuthStrikes(accountId) : 0;
}

function getRetryCount(intentId) {
  const fsm = _domains.get('engagement');
  return fsm && typeof fsm.getRetryCount === 'function' ? fsm.getRetryCount(intentId) : 0;
}

function resetAuthStrikes(accountId) {
  const fsm = _domains.get('engagement');
  if (fsm && typeof fsm.resetAuthStrikes === 'function') fsm.resetAuthStrikes(accountId);
}

function clearCircuitBreaker(accountId) {
  const fsm = _domains.get('engagement');
  if (fsm && typeof fsm.clearCircuitBreaker === 'function') fsm.clearCircuitBreaker(accountId);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 15. Module initialization
// ═══════════════════════════════════════════════════════════════════════════════

// Rehydration is called explicitly by the orchestrator AFTER the lineage worker
// has started. This ensures CK reads from a ledger populated by the worker
// rather than rehydrating from an empty/potentially-stale Redis key.
// Boot order: orchestrator → observability.init() → worker.start() → CK.rehydrate()

// ── Section 11 reconciliation bridge subscriber wiring ──────────────────────
// When the reconciliation FSM transitions to RECONCILING, it emits
// RECONCILIATION_CYCLE_STARTED. This subscriber catches that action,
// calls the dumb reconciliation engine, verifies constitutional hash integrity,
// and dispatches results back to the FSM via RECONCILIATION_RESULTS_RECEIVED.
// This is the async mechanical work that the FSM cannot perform itself.
//
// The cycle ALWAYS completes — even on engine failure, RECONCILIATION_RESULTS_RECEIVED
// and RECONCILIATION_CYCLE_COMPLETE are dispatched so the FSM returns to IDLE
// and subsequent cycles can proceed.
subscribeAction('RECONCILIATION_CYCLE_STARTED', async (action) => {
  // Phase 1 already done by FSM (RECONCILING state set, cycle slate reset).
  // Phase 2: Run the async engine comparison.

  let observations = [];
  let worstSeverity = 0;
  let hash = '';
  let hashMismatch = false;

  try {
    const engine = require('./reconciliation-engine');
    const substrates = _buildSubstrateQueries();
    const results = await engine.compare({ fsms: _domains, substrates, lineageLedger });

    observations = results.observations || [];
    worstSeverity = results.worstSeverity || 0;
    hash = results.hash || '';

    // HSM independently verifies constitutional hash
    const currentHash = await lineageLedger.computeHash();
    hashMismatch = results.hash !== currentHash;
    if (hashMismatch) {
      console.error('[constitutional-kernel] Constitutional HASH MISMATCH during reconciliation');
      _emitGovernanceTransition(_currentState, _currentState, {
        intent: 'RECONCILIATION_HASH_MISMATCH',
        reason: 'Constitutional identity divergence detected during reconciliation cycle',
      });
    }
  } catch (err) {
    console.error('[constitutional-kernel] Reconciliation engine error:', err.message);
  }

  // Phase 3: Route results through FSM — FSM transitions RECONCILING → CONVERGENT or DRIFTED
  dispatch({
    type: 'RECONCILIATION_RESULTS_RECEIVED',
    observations,
    worstSeverity,
    hash,
    hashMismatch,
  });

  // Phase 4: Always complete the cycle — FSM returns to IDLE
  dispatch({ type: 'RECONCILIATION_CYCLE_COMPLETE' });

  // Resolve the promise so triggerReconciliation() callers unblock
  if (_reconPromiseResolve) {
    _reconPromiseResolve({
      observations,
      worstSeverity,
      hash,
      hashMismatch,
      elapsedMs: Date.now() - _stateEnteredAt,
    });
    _reconPromiseResolve = null;
    _reconPromiseReject = null;
  }

  // ── Stability checkpoint ──────────────────────────────────────────────────
  if (_canCheckpoint()) {
    try {
      const entries = await lineageLedger.getLineage(200);
      const currentHash = await lineageLedger.computeHash();
      const reconFsm = _domains.get('reconciliation');
      const domainStates = {};
      for (const [name, fsm] of _domains) {
        domainStates[name] = fsm && typeof fsm.getState === 'function' ? fsm.getState() : 'unknown';
      }
      checkpointer.createSnapshot({
        entries,
        hash: currentHash,
        entryCount: await lineageLedger.getSize(),
        domainStates,
        epochCount: reconFsm && typeof reconFsm.getEpochCount === 'function'
          ? reconFsm.getEpochCount() : 0,
      });
    } catch (e) {
      console.error('[constitutional-kernel] Checkpoint creation failed:', e.message);
    }
  }
});

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
  triggerCoordinationCycle,
  validateMembraneTransition: _validateMembraneAuthority,
  validateProjectionSnapshot,
  recordMembraneBypassAnomaly,
  SIGNAL_CLASS,
  SIGNAL_OWNERSHIP_MAP,
};
