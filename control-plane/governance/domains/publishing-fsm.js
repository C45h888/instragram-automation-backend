// control-plane/governance/domains/publishing-fsm.js
// Publishing Domain FSM: federated state machine governing publishing lifecycle.
//
// Owns: signal buffering → evaluation → emission pipeline lifecycle,
//        backpressure detection, emission health.
// Does NOT own: evaluation policy (publishing policy), dedup logic,
//               intent construction, emission mechanics — those are
//               implementation concerns of the evaluation/emission modules.
//
// Reports to: constitutional kernel for transition validation + global observability.
//
// Local states:
//   IDLE       — no publishing events in flight
//   BUFFERING  — accumulating signal events in buffer
//   EVALUATING — evaluating buffered events against publishing policy
//   EMITTING   — emitting publishing intents to Redis queues

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No publishing events in flight — ready for signal intake',
  },
  BUFFERING: {
    description: 'Accumulating signal events in buffer',
  },
  EVALUATING: {
    description: 'Evaluating buffered events against publishing policy',
  },
  EMITTING: {
    description: 'Emitting publishing intents to Redis queues',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Signal ingested → begin buffering ──────────────────────────────────
  BUFFER_EVENT_INGESTED: {
    target: 'BUFFERING',
    guard: (event) => {
      if (!['IDLE', 'BUFFERING'].includes(_localState)) {
        return { allowed: false, reason: `Cannot buffer from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: () => [],
  },

  // ── Buffer full → begin evaluation ─────────────────────────────────────
  BUFFER_FLUSH_READY: {
    target: 'EVALUATING',
    guard: (event) => {
      if (!['IDLE', 'BUFFERING'].includes(_localState)) {
        return { allowed: false, reason: `Cannot evaluate from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => [{
      type: 'EVALUATE',
      accountId: event.accountId,
      events: event.events,
    }],
  },

  // ── Emission result observed ───────────────────────────────────────────
  EMISSION_OBSERVATION: {
    target: (event) => {
      if (event.status === 'error') return 'IDLE'; // error → idle, degradation reported separately
      return 'IDLE'; // success or empty → back to idle
    },
    guard: (event) => {
      if (!['EVALUATING', 'EMITTING'].includes(_localState)) {
        return { allowed: false };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      if (event.status === 'error') {
        return [
          { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: event.metadata?.reason || 'Emission failed' },
          { type: 'STOP_INTENT_DISCOVERY' },
        ];
      }
      return [{ type: 'START_INTENT_DISCOVERY' }];
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
    authority: 'publishing-fsm',
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
      authority: 'publishing-fsm',
      layer: 'domain',
      intent: event.type,
      priorState: from,
      resultantState: target,
      meta: { accountId: event.accountId || null, eventCount: event.eventCount || null },
    });
  }

  console.log(`[publishing-fsm] ${from} → ${target}  (${event.type})  [${lineageEntry.id.slice(0, 8)}]`);

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
  return { ok: _localState !== 'EMITTING', signals: { state: _localState } };
}

module.exports = {
  name: 'publishing',
  dispatch,
  getState,
  getLineage,
  exportState,
  getHealth,
};
