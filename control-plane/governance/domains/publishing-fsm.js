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
// Architectural invariant:
//   Signals UP   → ctx.dispatchGlobal(event) reports degradation to constitutional
//   Authority ↓  → ctx.validate(from, to, event) asks constitutional for approval
//   Membranes ↓  → actions returned to constitutional for emission to orchestrators

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}
//   Lineage      → ctx.recordLineage() writes to authoritative ledger (via CK mediation)
//
// Domain FSMs CANNOT directly access the lineage ledger.
// The constitutional kernel mediates all lineage writes.
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
  PUBLISHING: {
    description: 'DB scan emitted intents — mutation substrate applying APPROVED→PUBLISHING transition',
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
      return 'IDLE'; // always return to IDLE — error state collapses to idle
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

  // ── DB scan emitted intent → apply APPROVED→PUBLISHING mutation ─────────
  // Scanner emits DB_SCAN_EMITTED after LPUSHing intent to Redis.
  // This transition authorizes the DB status update via mutation-substrate.
  DB_SCAN_EMITTED: {
    target: 'PUBLISHING',
    guard: (event) => {
      // Guard: event must have required fields for mutation
      if (!event?.target || !event?.recordId) {
        return { allowed: false, reason: 'DB_SCAN_EMITTED requires target and recordId' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { target, recordId, accountId, actionType, currentStatus } = event;

      if (target === 'scheduled_post') {
        // scheduled_posts: APPROVED → PUBLISHING
        return [{
          type: 'APPLY_MUTATION',
          table: 'scheduled_posts',
          recordId,
          accountId,
          updates: { status: 'publishing' },
          expectedPriorStatus: 'approved',
          reason: `Scanner emitted intent for scheduled_post ${recordId}`,
        }];
      }

      if (target === 'post_queue') {
        // post_queue: (pending|failed) → processing
        return [{
          type: 'APPLY_MUTATION',
          table: 'post_queue',
          recordId,
          accountId,
          updates: { status: 'processing' },
          expectedPriorStatus: currentStatus || 'pending',
          reason: `Scanner emitted intent for post_queue row ${recordId}`,
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
// Domain FSMs emit state transitions through the observability plane.
// Lineage authority is held by the lineage worker (Phase 2).
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

  // Ask constitutional kernel for approval
  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  _localState = target;

  // Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'publishing',
        entity: 'fsm',
        entityId: 'publishing-fsm',
        previousState: from,
        nextState: target,
        authority: 'publishing-fsm',
        raw: { intent: event.type, accountId: event.accountId || null, eventCount: event.eventCount || null },
      });
    }
  } catch (_) {}

  const actions = txn.buildActions ? txn.buildActions(event) : [];

  console.log(`[publishing-fsm] ${from} → ${target}  (${event.type})`);

  return {
    allowed: true,
    from,
    to: target,
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
 * @param {string} rehydratedState — the domain state to restore (e.g., 'BUFFERING', 'IDLE')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[publishing-fsm] Initialized with rehydrated state: ${rehydratedState}`);
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
  return { ok: _localState !== 'EMITTING', signals: { state: _localState } };
}

module.exports = {
  name: 'publishing',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
};
