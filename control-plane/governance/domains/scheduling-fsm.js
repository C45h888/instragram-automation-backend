// control-plane/governance/domains/scheduling-fsm.js
// Scheduling Domain FSM: federated state machine governing maintenance cadence.
//
// Owns: cadence-driven maintenance lifecycle (scan → refresh → check → metrics),
//        worker metrics evaluation, health signal reporting.
// Does NOT own: database scanning mechanics, lifecycle discovery,
//               safety checks, metrics collection — those are
//               implementation concerns of runtime substrates.
//
// Reports to: constitutional kernel for transition validation and global observability.
//
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) reports degradation to constitutional
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Membranes ↓  → actions returned to constitutional for emission to orchestrators
//   Lineage      → ctx.recordLineage() writes to authoritative ledger (via CK mediation)
//
// Domain FSMs CANNOT directly access the lineage ledger.
// The constitutional kernel mediates all lineage writes.
//
// Local states:
//   IDLE       — between cadence cycles
//   SCANNING   — scanning database for publishable items
//   REFRESHING — refreshing account lifecycle
//   CHECKING   — running safety checks and metrics

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'Between cadence cycles — awaiting next CADENCE_TICK',
  },
  SCANNING: {
    description: 'Scanning database for publishable items',
  },
  REFRESHING: {
    description: 'Refreshing account lifecycle — discovering new/removed accounts',
  },
  CHECKING: {
    description: 'Running safety checks and collecting worker metrics',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Cadence tick → sequence maintenance actions ────────────────────────
  CADENCE_TICK: {
    target: () => _localState, // no state change — sequences actions
    guard: () => ({ allowed: true }),
    buildActions: () => [
      { type: 'SCAN_DATABASE' },
      { type: 'REFRESH_LIFECYCLE' },
      { type: 'CHECK_SAFETY' },
      { type: 'REPORT_METRICS' },
    ],
  },

  // ── Maintenance acknowledgements ────────────────────────────────────────
  DATABASE_SCANNED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: () => [],
  },

  LIFECYCLE_REFRESHED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      if (event.accountIds && event.accountIds.length > 0) {
        return [{ type: 'UPDATE_ACCOUNTS', accountIds: event.accountIds }];
      }
      return [];
    },
  },

  SAFETY_CHECK_COMPLETE: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: () => [],
  },

  // ── Worker metrics — domain evaluates health, reports to constitutional ─
  // Policy: unhealthy when total >= 5 samples AND failureRate >= 50%
  WORKER_METRICS_REPORTED: {
    target: () => {
      return _localState; // no state change
    },
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      if (event.total >= 5 && event.failureRate >= 0.5) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'RETRY_PRESSURE',
          reason: `Worker failure rate ${Math.round(event.failureRate * 100)}% (${event.failed}/${event.total} in ${((event.windowMs || 60000) / 1000).toFixed(0)}s)`,
        }];
      }
      return [];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch
//
// Write order invariant (Lineage-First):
//   1. ctx.recordLineage() — write to authoritative ledger via CK mediation
//   2. _localState mutation — then materialize domain state
//
// Domain FSMs CANNOT directly access the lineage ledger.
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
    // Record lineage first
    if (ctx && ctx.recordLineage) {
      ctx.recordLineage({
        authority: 'scheduling-fsm',
        layer: 'domain',
        intent: event.type,
        priorState: from,
        resultantState: from,
        meta: {},
      });
    }
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition: event recorded' };
  }

  // Ask constitutional kernel for approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // LINEAGE FIRST — record to authoritative ledger before mutating state
  let lineageId = null;
  if (ctx && ctx.recordLineage) {
    const entry = {
      authority: 'scheduling-fsm',
      layer: 'domain',
      intent: event.type,
      priorState: from,
      resultantState: target,
      meta: {},
    };
    const recorded = ctx.recordLineage(entry);
    lineageId = recorded.id || recorded.lineageId || null;
  }

  _localState = target;
  const actions = txn.buildActions ? txn.buildActions(event) : [];

  console.log(`[scheduling-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
    lineageId,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * Called by the constitutional kernel after rehydrate() completes on boot.
 *
 * @param {string} rehydratedState — the domain state to restore (e.g., 'SCANNING', 'IDLE')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[scheduling-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return { state: _localState };
}

function getHealth() {
  return { ok: true, signals: {} };
}

module.exports = {
  name: 'scheduling',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
};
