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
const ingressSubstrate = require('./ingress-consistency/substrate');

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
  PARSING_DISPATCHED: 'acquisition',
  PARSING_COMPLETE: 'acquisition',

  // Engagement domain — circuit breaker, auth strikes, retry counting
  AUTH_FAILURE_STRIKE: 'engagement',
  RATE_LIMIT_DETECTED: 'engagement',
  RATE_LIMIT_CLEARED: 'engagement',
  RETRY_EXHAUSTED: 'engagement',
  CIRCUIT_BREAKER_CHECK: 'engagement',
  CIRCUIT_COOLDOWN_ELAPSED: 'engagement',
  CIRCUIT_TEST_SUCCESS: 'engagement',
  CIRCUIT_TEST_FAIL: 'engagement',
  CIRCUIT_BREAKER_CLEARED: 'engagement',
  AUTH_STRIKES_RESET: 'engagement',
  AUTH_SUCCESS: 'engagement',
  RETRY_REQUESTED: 'engagement',

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
  RECONCILIATION_CYCLE_COMPLETE: 'reconciliation',

  // Persist-Telemetry domain — governs all DB write operations
  DB_WRITE_REQUESTED: 'persist-telemetry',
  DB_WRITE_COMPLETE: 'persist-telemetry',
  DB_READ_OBSERVED: 'persist-telemetry',

  // Telemetry Coordination domain — deterministic semantic ingress plane
  TELEMETRY_PROCESS_INTENTS: 'telemetry-coordination', // reactive trigger (routes through CK then to FSM)
  PROCESS_INTENTS: 'telemetry-coordination',
  HALT_TELEMETRY_COORDINATION: 'telemetry-coordination',
  RESUME_TELEMETRY_COORDINATION: 'telemetry-coordination',
  // Phase 3: CK async validation — Phase 2 worker notifies CK post-write
  PROJECTION_PERSISTED: 'telemetry-coordination',
  // Ingress lag retry orchestration
  INGRESS_RETRY_REQUESTED: 'telemetry-coordination',
  INGRESS_RESOLVED: 'telemetry-coordination',
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

  // ── Ingress Consistency — lag detected by substrate, signals CK ────────────
  // INGRESS_STATE_CHANGED is a global event (not routed to any domain FSM).
  // CK handles it by escalating to telemetry-coordination for retry orchestration.
  INGRESS_STATE_CHANGED: {
    target: () => null, // CK does not change its own state from this event
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      const { lag, status } = event;

      // Track ingestion lag state internally
      _ingestionLagState = { lag, status, lastUpdate: Date.now() };

      if (status === 'CRITICAL' || status === 'DEGRADED') {
        // Escalate: dispatch INGRESS_RETRY_REQUESTED to telemetry-coordination domain
        return [{ type: 'INGRESS_RETRY_REQUESTED', lag, status, timestamp: Date.now() }];
      }

      if (status === 'CONSISTENT' && _ingestionLagState && _ingestionLagState.lag > 0) {
        // Lag cleared — inform telemetry-coordination to wind down retry budget
        return [{ type: 'INGRESS_RESOLVED', lag: 0, status: 'CONSISTENT' }];
      }

      return [];
    },
  },

  // ── Reactive Telemetry Coordination — FSM onWrite path now routes through CK ──
  // When FSM's reactive onWrite trigger fires (on PROJECTION_INTENT entries),
  // it calls _ckContext.dispatchGlobal({ type: 'TELEMETRY_PROCESS_INTENTS' }).
  // This handler dispatches to the telemetry-coordination FSM, giving CK full
  // visibility and sequencing control over every reactive coordination cycle.
  // CK does not change its own state — this is purely a routing/observability event.
  TELEMETRY_PROCESS_INTENTS: {
    target: () => null, // CK does not change its own state
    guard: (event, ctx) => {
      if (_currentState === 'HALTED' || _currentState === 'DEAD') {
        return { allowed: false, reason: `CK is ${_currentState} — telemetry coordination blocked` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const fsm = _domains.get('telemetry-coordination');
      if (fsm) {
        // Forward to FSM — FSM handles PROCESS_INTENTS logic
        fsm.dispatch({ type: 'PROCESS_INTENTS', source: event.source || 'reactive' }, ctx);
      }
      // CK does not emit actions — FSM emits COORDINATION_CYCLE_COMPLETE etc.
      // which return to CK as normal domain FSM actions.
      return [];
    },
  },

  // ── Transition Writer Health — worker layer degraded ─────────────────────
  // Fired by ingress-consistency substrate when transition-writer health is not OK.
  // CK treats this as an infrastructure alert — tracks state but does not
  // transition CK's own lifecycle state. Informational for observability.
  TRANSITION_WRITER_HEALTH_CHANGED: {
    target: () => null, // CK does not change lifecycle state from this event
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      const { namespace, health } = event;

      // Track aggregate writer health state internally for diagnostics
      _ingestionLagState = {
        ..._ingestionLagState,
        writerStatus: health.status,
        writerLastError: health.writers?.find(w => !w.ok)?.lastError ?? null,
        writerLastErrorCategory: health.writers?.find(w => !w.ok)?.lastErrorCategory ?? null,
        lastUpdate: Date.now(),
      };

      if (health.status === 'FAILED' || health.status === 'STOPPED') {
        // Critical: all writers dead — escalate through telemetry retry chain
        return [
          {
            type: 'TRANSITION_WRITER_FAILED',
            namespace: namespace || 'aggregate',
            health,
          },
          {
            type: 'INGRESS_RETRY_REQUESTED',
            lag: health.totalErrors || 0,
            status: 'CRITICAL',
            source: 'transition_writer',
            timestamp: Date.now(),
          },
        ];
      }

      if (health.status === 'DEGRADED') {
        // Degraded: some writes failing — signal for retry cadence
        return [
          {
            type: 'TRANSITION_WRITER_DEGRADED',
            namespace: namespace || 'aggregate',
            health,
          },
        ];
      }

      // Writer recovered — log resolved
      if (health.status === 'OK' && _ingestionLagState?.writerStatus !== 'OK') {
        return [
          {
            type: 'TRANSITION_WRITER_RECOVERED',
            namespace: namespace || 'aggregate',
            totalWrites: health.totalWrites,
          },
        ];
      }

      return [];
    },
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

// Snapshot building moved to reconciliation-substrate.js — CK no longer captures it

let _accountIds = [];

// Domain registry
const _domains = new Map(); // domainName → fsm

// Rehydrated domain states — populated during rehydrate() from lineage
let _rehydratedDomainStates = null;

// Ingress consistency lag state — updated by INGRESS_STATE_CHANGED handler
// Not a CK state (CK doesn't transition on lag), just tracking for G5 gate and diagnostics
let _ingestionLagState = { lag: 0, status: 'CONSISTENT', lastUpdate: 0 };

// Action subscription
const _actionSubscribers = new Map(); // actionType → Set<fn>
let _legacyActionSubscriber = null;

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
      // init() may be async (e.g. telemetry-coordination-fsm reads Redis-backed cursor).
      // Fire-and-forget is safe here: cursor restoration is idempotent and init is called
      // before any reactive processing begins.
      fsm.init(rehydratedState).catch((err) => {
        console.error(`[constitutional-kernel] Domain '${fsm.name}' init error:`, err.message);
      });
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

// Reconciliation substrate — owns snapshot building, substrate queries,
// checkpoint gate evaluation, and worker orchestration.
// CK remains HSM authority — dispatches FSM transitions after substrate returns.

const reconciliationSubstrate = require('./reconciliation-substrate');

// Reconciliation substrate owns this — moved to reconciliation-substrate.js

/**
 * Ask the reconciliation FSM if a new cycle may be started.
/**
 * Ask the reconciliation FSM for trigger evaluation.
 *
 * FSM evaluates deterministically: IDLE check, anti-thrash (MIN_INTERVAL),
 * drift state, trigger type, and escalation criteria. CK applies its own
 * structural sanity gates on top of FSM's response before dispatch.
 *
 * Returns the raw FSM result so CK can validate before deciding.
 *
 * @param {{ trigger?: string, forced?: boolean }} options
 * @returns {{ decision: string, reason: string, retryAt?: number, fsmState: string }}
 */
function _askReconciliationFSM(options = {}) {
  const { trigger = 'MANUAL', forced = false } = options;
  const reconFsm = _domains.get('reconciliation');
  if (!reconFsm || typeof reconFsm.evaluateTriggerCriteria !== 'function') {
    return { decision: 'DENIED', reason: 'Reconciliation FSM missing evaluateTriggerCriteria()', fsmState: 'UNKNOWN' };
  }
  const result = reconFsm.evaluateTriggerCriteria({ trigger, forced });
  return {
    decision: result.decision,
    reason: result.reason,
    retryAt: result.retryAt,
    fsmState: reconFsm.getState(),
  };
}

/**
 * CK self-validation: structural sanity gates applied to FSM's response.
 *
 * These are NOT a second evaluation of trigger criteria — that is FSM's domain.
 * These are CK's sovereign sanity checks to ensure the central authority plane
 * retains control over the dispatch decision.
 *
 * G1 — FSM existence and callable interface
 * G2 — Decision shape sanity (known decision values only)
 * G3 — FSM state consistency (must be IDLE to accept APPROVED)
 * G4 — Trigger type sanity (known trigger types only)
 * G5 — Forced flag non-re-entrancy
 *
 * @param {{ decision: string, reason: string, retryAt?: number, fsmState: string }} fsmResult
 * @param {{ trigger: string, forced: boolean }} options
 * @returns {{ allowed: boolean, override: boolean, reason: string }}
 */
function _ckSelfValidation(fsmResult, options = {}) {
  const { trigger, forced } = options;
  const { decision, fsmState } = fsmResult;

  // G1: FSM returned a valid decision shape
  const validDecisions = ['APPROVED', 'DENIED', 'WAIT'];
  if (!validDecisions.includes(decision)) {
    return { allowed: false, override: true, reason: `G1: FSM returned invalid decision "${decision}"` };
  }

  // G2: FSM state sanity — APPROVED requires IDLE state
  // If FSM says APPROVED but is not IDLE, CK overrides and denies
  if (decision === 'APPROVED' && fsmState !== 'IDLE') {
    return { allowed: false, override: true, reason: `G2: FSM returned APPROVED but state is "${fsmState}" (expected IDLE)` };
  }

  // G3: Known trigger types
  const knownTriggers = ['MANUAL', 'LOG_DEGRADED', 'ESCALATION_SIGNAL', 'INGRESS_LAG'];
  if (!knownTriggers.includes(trigger)) {
    return { allowed: false, override: true, reason: `G3: Unknown trigger type "${trigger}"` };
  }

  // G4: Forced flag non-re-entrancy — CK tracks own in-progress flag
  if (forced && _reconInProgress) {
    return { allowed: false, override: true, reason: 'G4: Forced reconciliation already in progress' };
  }

  // G5: If forced=true, FSM must have approved it (FSM handles anti-thrash bypass internally)
  // CK does not double-check FSM's forced logic — only structural sanity above

  return { allowed: true, override: false, reason: fsmResult.reason };
}

/**
 * Internal: dispatch the full reconciliation cycle.
 * Called by triggerReconciliation() after FSM approves, or directly for T3 (death).
 *
 * @param {{ forced?: boolean }} options
 */
function _dispatchReconciliationCycle({ forced = false } = {}) {
  const reconFsm = _domains.get('reconciliation');

  // ── Dispatch TICK — FSM transitions IDLE → RECONCILING ─────────────────────
  dispatch({ type: 'RECONCILIATION_TICK' });

  // Verify FSM accepted (guard may have denied synchronously)
  if (reconFsm.getState() === 'IDLE') {
    console.warn('[CK] FSM rejected RECONCILIATION_TICK — skipping substrate');
    return;
  }

  // ── Substrate runs the full cycle (snapshot + engine + checkpoint gate) ─────
  let result;
  try {
    result = reconciliationSubstrate.triggerCycle({
      fsms: _domains,
      currentState: _currentState,
    });
  } catch (err) {
    console.error('[CK] Reconciliation substrate error:', err.message);
    result = null;
  }

  // ── Always complete the cycle — FSM must return to IDLE ────────────────────
  dispatch({
    type: 'RECONCILIATION_RESULTS_RECEIVED',
    observations: result ? result.observations : [],
    worstSeverity: result ? result.worstSeverity : 0,
    hash: result ? result.hash : '',
  });

  dispatch({ type: 'RECONCILIATION_CYCLE_COMPLETE' });
}

/**
 * Trigger a complete reconciliation cycle.
 *
 * CK receives a trigger signal (T1/T2/T3/T5), asks FSM for evaluation,
 * and dispatches only if FSM approves — except T3 (death), which is
 * non-negotiable and bypasses FSM evaluation entirely.
 *
 * T1 — Domain drift:     CK receives LOG_DEGRADED → _askReconciliationFSM({ trigger: 'LOG_DEGRADED' }) → dispatches if approved
 * T2 — Escalation:        FSM recommends via ctx.dispatchGlobal() → CK's GLOBAL_TRANSITION_MAP triggers → _askReconciliationFSM({ trigger: 'ESCALATION_SIGNAL' }) → dispatches if approved
 * T3 — Death/corruption:  CK detects → _dispatchReconciliationCycle({ forced: true }) directly, no FSM ask
 * T5 — Manual/forced:     CK.triggerReconciliation({ forced: true }) → _askReconciliationFSM({ trigger: 'MANUAL', forced: true }) → dispatches
 *
 * Constitutional invariant:
 *   The cycle ALWAYS completes. Even on engine failure, _dispatchReconciliationCycle()
 *   is called so the FSM returns to IDLE.
 *
 * @param {{ forced?: boolean }} options
 * @returns {Promise<{ observations, worstSeverity, hash, snapshotHash, elapsedMs }>|null}
 */
async function triggerReconciliation({ forced = false } = {}) {
  // ── T3: Death is non-negotiable — CK dispatches directly, no FSM evaluation ─
  const ledgerSize = await lineageLedger.getSize();
  const ckpt = checkpointer.getCheckpoint();
  if (_detectConstitutionalDeath(ledgerSize, ckpt)) {
    console.log('[CK] Constitutional death detected — dispatching emergency reconciliation');
    _dispatchReconciliationCycle({ forced: true });
    return null;
  }

  // ── Ask FSM for trigger evaluation ─────────────────────────────────────────
  const fsmResult = _askReconciliationFSM({ trigger: 'MANUAL', forced });

  // ── CK self-validation: sovereign sanity gates ─────────────────────────────
  const ckValidation = _ckSelfValidation(fsmResult, { trigger: 'MANUAL', forced });

  if (!ckValidation.allowed) {
    if (ckValidation.override) {
      console.log(`[CK] Override — ${ckValidation.reason}`);
    } else if (fsmResult.decision === 'WAIT' && fsmResult.retryAt) {
      console.log(`[CK] FSM requested wait: ${fsmResult.reason} (retryAt: ${fsmResult.retryAt})`);
    } else {
      console.log(`[CK] FSM denied trigger: ${fsmResult.reason}`);
    }
    return null;
  }

  // APPROVED — dispatch the cycle
  _dispatchReconciliationCycle({ forced });
  return null;
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

// _canCheckpoint moved to reconciliation-substrate.js — CK no longer owns checkpoint gate evaluation

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

  // 5. Rehydrate CK from checkpoint's pre-computed ACCEPTED-only domainStates
  // No need to re-materialize from Redis — ckpt.domainStates already reflects
  // ACCEPTED-only materialization from the last stable checkpoint.
  if (ckpt.domainStates) {
    _currentState = ckpt.domainStates.governance || 'BOOTING';
    _rehydratedDomainStates = ckpt.domainStates;
    console.log(`[CK] Rehydrated from checkpoint: globalState='${_currentState}', domains=${JSON.stringify(_rehydratedDomainStates)}`);
  } else {
    await rehydrate(); // fallback to Redis materialization
  }

  // 6. Re-register domains with rehydrated states
  for (const [name, fsm] of _domains) {
    const state = _rehydratedDomainStates ? _rehydratedDomainStates[name] : null;
    if (state && typeof fsm.init === 'function') {
      // Same fire-and-forget pattern as registerDomain — init may be async
      fsm.init(state).catch((err) => {
        console.error(`[CK] Domain '${name}' init error:`, err.message);
      });
      console.log(`[CK] Domain '${name}' re-initialized with checkpoint state: ${state}`);
    }
  }

  // 7. Restart workers
  try { await require('../telemetry-workers').startAll(); } catch (e) { console.warn('[CK] Telemetry workers restart error:', e.message); }
  

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

  // ── Reactive reconciliation trigger — LOG_DEGRADED subscriber ─────────────────
  // Initialized here (after rehydrate so all domains are registered) rather than
  // in triggerReconciliation() hot-path. T1 reactive trigger path.
  _initReconciliationTrigger();
}

// ── Reconciliation reactive trigger initialization ──────────────────────────

let _reconciliationTriggerInitialized = false;

/**
 * Register the LOG_DEGRADED subscriber for reconciliation.
 * Called once from rehydrate() after all domains are registered.
 * This is the T1 reactive trigger path — domain drift fires LOG_DEGRADED,
 * which arrives here, is evaluated by FSM.evaluateTriggerCriteria(), and
 * CK dispatches only if FSM approves.
 */
function _initReconciliationTrigger() {
  if (_reconciliationTriggerInitialized) return;
  _reconciliationTriggerInitialized = true;

  subscribeAction('LOG_DEGRADED', (event) => {
    const fsmResult = _askReconciliationFSM({ trigger: 'LOG_DEGRADED', forced: false });
    const ckValidation = _ckSelfValidation(fsmResult, { trigger: 'LOG_DEGRADED', forced: false });
    if (ckValidation.allowed) {
      _dispatchReconciliationCycle({ forced: false });
    } else if (ckValidation.override) {
      console.log(`[CK] LOG_DEGRADED override — ${ckValidation.reason}`);
    }
    // DENIED or WAIT — reconciliation deferred, domain continues without CK intervention
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// 13. Observability
// ═══════════════════════════════════════════════════════════════════════════════

// ── Public diagnostic API ──────────────────────────────────────────────────

/**
 * Return the current aggregate health of the transition-writer layer.
 * Used by ingress-consistency substrate to determine if writers are operational.
 * @returns {object} aggregate health from transition-writers/index.js
 */
function getTransitionWriterHealth() {
  try {
    // eslint-disable-next-line global-require
    const tw = require('../telemetry-workers/transition-writers');
    return tw.getHealth();
  } catch (err) {
    return { status: 'UNAVAILABLE', error: err.message, timestamp: Date.now() };
  }
}

/**
 * Return the current ingress/consumption lag state tracked by CK.
 * Updated on every INGRESS_STATE_CHANGED or TRANSITION_WRITER_HEALTH_CHANGED.
 * @returns {object} lag state
 */
function getIngressState() {
  return { ..._ingestionLagState };
}

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

// ── Module initialization ──────────────────────────────────────────────────────
// Reconciliation FSM owns trigger criteria (IDLE + MIN_INTERVAL + no drift).
// CK dispatches RECONCILIATION_TICK — FSM evaluates criteria before accepting.
// Reconciliation substrate owns snapshot building, engine comparison,
// checkpoint gate, and worker orchestration.
// See: reconciliation-substrate.js and reconciliation-worker.js

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
  validateMembraneTransition: _validateMembraneAuthority,
  recordMembraneBypassAnomaly,
  SIGNAL_CLASS,
  getTransitionWriterHealth,
  getIngressState,
};
