// control-plane/governance/domains/publishing-fsm.js
// Publishing Domain FSM: federated state machine governing publishing lifecycle.
//
// Owns: deterministic trigger intake (via cognition scanner) → governed read → evaluation → emission,
//        backpressure detection, emission health.
//
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
//
// Trigger protocol (pull-based, reader-driven):
//   1. Cognition scanner detects Realtime UPDATE → dispatches PUBLISHING_DATA_AVAILABLE
//   2. FSM transitions IDLE → FETCHING, emits GOVERNED_READ actions
//   3. GOVERNED_READ → CK → persist-telemetry-fsm → reading-substrate → post-queue-worker
//   4. post-queue-worker queries Supabase for pending post_queue + approved scheduled_posts
//   5. Result returns as READ_RESULT_AVAILABLE → FSM transitions FETCHING → EVALUATING
//   6. EVALUATE action → emission-orchestrator → evaluator → emitter
//   7. EMISSION_OBSERVATION → IDLE → cognition scanner flushes next batch
//
// Local states:
//   IDLE       — waiting for cognition-scanner trigger
//   FETCHING   — governed DB read in flight (reading post_queue + scheduled_posts)
//   EVALUATING — evaluating fetched events against publishing policy

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'Waiting for cognition-scanner trigger — ready to fetch',
  },
  FETCHING: {
    description: 'Governed DB read in flight — pulling post_queue + scheduled_posts',
  },
  EVALUATING: {
    description: 'Running evaluation pipeline against publishing policy',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  // ── Cognition scanner triggered → begin governed read ───────────────────
  PUBLISHING_DATA_AVAILABLE: {
    target: 'FETCHING',
    guard: (event) => {
      if (_localState !== 'IDLE') {
        return { allowed: false, reason: `Cannot fetch from ${_localState}` };
      }
      if (!event?.accountId) {
        return { allowed: false, reason: 'PUBLISHING_DATA_AVAILABLE requires accountId' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId } = event;
      const readId = crypto.randomUUID();
      // Issue two governed reads: post_queue (pending/failed) + scheduled_posts (approved)
      // persist-telemetry-fsm gates these through DB_READ_REQUESTED
      return [
        {
          type: 'GOVERNED_READ',
          readDomain: 'db.post-queue',
          accountId,
          readId: `${readId}:pq`,
          params: { accountId, query: 'getPendingPostQueue' },
        },
        {
          type: 'GOVERNED_READ',
          readDomain: 'db.scheduled-posts',
          accountId,
          readId: `${readId}:sp`,
          params: { accountId, query: 'getApprovedScheduledPosts' },
        },
      ];
    },
  },

  // ── Governed read completed → begin evaluation ─────────────────────────
  READ_RESULT_AVAILABLE: {
    target: 'EVALUATING',
    guard: (event) => {
      if (_localState !== 'FETCHING') {
        return { allowed: false, reason: `Cannot evaluate from ${_localState}` };
      }
      if (!event?.accountId) {
        return { allowed: false, reason: 'READ_RESULT_AVAILABLE requires accountId' };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId, readDomain, data } = event;
      // Transform DB rows into evaluator-compatible { table, record } events.
      // readDomain tells us which table the data came from.
      const table = readDomain === 'db.scheduled-posts' ? 'scheduled_posts' : 'post_queue';
      const events = (data && Array.isArray(data))
        ? data.map(record => ({ table, record }))
        : [];

      return [{
        type: 'EVALUATE',
        accountId,
        events,
      }];
    },
  },

  // ── Emission result observed ────────────────────────────────────────────
  EMISSION_OBSERVATION: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'EVALUATING') {
        return { allowed: false, reason: `Cannot complete emission from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      if (event.status === 'error') {
        return [
          { type: 'LOG_DEGRADED', substate: 'PARTIAL_FAILURE', reason: event.metadata?.reason || 'Emission failed' },
        ];
      }
      // Return to IDLE — cognition-scanner will flush next batch on EMISSION_OBSERVATION
      return [];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null;
let _pendingReads = new Map(); // readId → { domain, accountId } — tracks in-flight governed reads

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

  // ── Track in-flight governed reads ──────────────────────────────────────
  if (event.type === 'PUBLISHING_DATA_AVAILABLE') {
    // Reads will be tracked when GOVERNED_READ actions are processed
  }
  if (event.type === 'READ_RESULT_AVAILABLE' && event.readId) {
    _pendingReads.delete(event.readId);
  }

  _localState = target;
  _lastTransitionedAt = Date.now();

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
        raw: { intent: event.type, accountId: event.accountId || null },
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
 * @param {string} rehydratedState — the domain state to restore (e.g., 'IDLE', 'FETCHING', 'EVALUATING')
 */
function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[publishing-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Messaging Window Policy
//
// Pure policy function: computes 24h customer messaging window from last customer
// message timestamp. Called by governance-layer callers to derive window state
// from raw timestamps stored by the persistence substrate.
//
// Policy constants:
//   - Window duration: 24 hours
//   - When window is open: customer messaged within the last 24 hours
//   - Template required: when window is closed (cannot freely message)
// ═══════════════════════════════════════════════════════════════════════════════

const MESSAGING_WINDOW_HOURS = 24;

/**
 * Computes the 24h customer messaging window state from the last customer
 * message timestamp.
 *
 * @param {string|null} lastCustomerMessageAt - ISO8601 timestamp of customer's last message
 * @returns {{ is_open: boolean, hours_remaining: number|null, window_expires_at: string|null, can_send_messages: boolean, requires_template: boolean }}
 */
function computeMessagingWindow(lastCustomerMessageAt) {
  if (!lastCustomerMessageAt) {
    return {
      is_open: false,
      hours_remaining: null,
      window_expires_at: null,
      can_send_messages: false,
      requires_template: true,
    };
  }

  const lastMs = new Date(lastCustomerMessageAt).getTime();
  if (Number.isNaN(lastMs)) {
    return {
      is_open: false,
      hours_remaining: null,
      window_expires_at: null,
      can_send_messages: false,
      requires_template: true,
    };
  }
  const nowMs = Date.now();
  const hoursSince = (nowMs - lastMs) / (1000 * 60 * 60);

  if (hoursSince >= MESSAGING_WINDOW_HOURS) {
    return {
      is_open: false,
      hours_remaining: 0,
      window_expires_at: new Date(lastMs + MESSAGING_WINDOW_HOURS * 60 * 60 * 1000).toISOString(),
      can_send_messages: false,
      requires_template: true,
    };
  }

  const hoursRemaining = Math.max(0, MESSAGING_WINDOW_HOURS - hoursSince);
  const windowExpiresAt = new Date(nowMs + hoursRemaining * 60 * 60 * 1000).toISOString();

  return {
    is_open: true,
    hours_remaining: parseFloat(hoursRemaining.toFixed(3)),
    window_expires_at: windowExpiresAt,
    can_send_messages: true,
    requires_template: false,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Observability
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return { state: _localState, pendingReads: _pendingReads.size };
}

function getHealth() {
  return { ok: _localState !== 'EVALUATING', signals: { state: _localState, pendingReads: _pendingReads.size } };
}

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

module.exports = {
  name: 'publishing',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getLastTransitionedAt,
  computeMessagingWindow,
};
