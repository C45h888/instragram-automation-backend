// control-plane/governance/domains/scheduling-fsm.js
// Scheduling Domain FSM: federated state machine governing maintenance cadence.
//
// Owns: cadence-driven maintenance lifecycle (scan → refresh → check → metrics),
//        worker metrics evaluation, health signal reporting.
// Does NOT own: database scanning mechanics, lifecycle discovery,
//               safety checks, metrics collection — those are
//               implementation concerns of runtime substrates.
//
// Reports to: constitutional kernel for transition validation + global observability.
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
      if (_localState !== 'IDLE') return _localState;
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
const _domainLineage = [];

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Domain lineage recorder
// ═══════════════════════════════════════════════════════════════════════════════

function _recordDomainLineage(from, to, trigger, actions) {
  const entry = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    authority: 'scheduling-fsm',
    layer: 'domain',
    intent: trigger,
    priorState: from,
    resultantState: to,
    actions: actions.map(a => ({ type: a.type })),
  };
  _domainLineage.push(entry);
  return entry;
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
    _recordDomainLineage(from, from, event.type, []);
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition: event recorded' };
  }

  // Ask constitutional kernel for approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  _localState = target;
  const actions = txn.buildActions ? txn.buildActions(event) : [];
  const lineageEntry = _recordDomainLineage(from, target, event.type, actions);

  if (ctx && ctx.recordLineage) {
    ctx.recordLineage({
      authority: 'scheduling-fsm',
      layer: 'domain',
      intent: event.type,
      priorState: from,
      resultantState: target,
      meta: {},
    });
  }

  console.log(`[scheduling-fsm] ${from} → ${target}  (${event.type})  [${lineageEntry.id.slice(0, 8)}]`);

  return {
    allowed: true,
    from,
    to: target,
    lineageId: lineageEntry.id,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function getLineage(n) {
  if (typeof n === 'number' && n > 0) return _domainLineage.slice(-n);
  return [..._domainLineage];
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
  getState,
  getLineage,
  exportState,
  getHealth,
};
