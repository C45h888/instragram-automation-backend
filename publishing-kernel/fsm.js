// publishing-kernel/fsm.js
// Publishing Domain FSM: federated state machine governing publishing lifecycle.
//
// Owns: deterministic trigger intake (via cognition scanner) → governed read →
//        content-type classification → substrate dispatch → observation,
//        retry delegation to engagement-fsm via CK membrane.
//
// Does NOT own: evaluation policy (done by publishing policy in runtime/evaluation.js),
//               substrate execution, retry cadence (engagement-fsm domain),
//               credential resolution, IG API mechanics.
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
//   4. Result returns as READ_RESULT_AVAILABLE → FSM transitions FETCHING → EVALUATING
//   5. EVALUATING classifies content type → EXECUTING with EXECUTE_CONTENT or EXECUTE_ENGAGEMENT
//   6. Orchestrator executes via bounded substrate, signals back PUBLISHING_OBSERVATION
//   7. PUBLISHING_OBSERVATION → IDLE
//
// States:
//   IDLE       — waiting for cognition-scanner trigger
//   FETCHING   — governed DB read in flight
//   EVALUATING — classifying content type, dispatching to orchestrator
//   EXECUTING  — bounded substrate execution in flight (worker → IG API)

const crypto = require('crypto');

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

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
    description: 'Classifying content type — dispatching substrate action',
  },
  EXECUTING: {
    description: 'Bounded substrate execution in flight — calling IG Graph API',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Classify a row from the read result into (domain, worker, actionType).
 * post_queue.action_type determines engagement vs content.
 * scheduled_posts always map to content.
 */
function _classify(row) {
  const { table, record } = row;
  const actionType = record?.action_type;
  const mediaType = record?.media_type;

  // scheduled_posts → content (posts worker)
  if (table === 'scheduled_posts') {
    return { domain: 'content', worker: 'posts', actionType: 'publish_post' };
  }

  // post_queue → classify by action_type
  if (actionType === 'reply_comment') {
    return { domain: 'engagement', worker: 'comments', actionType };
  }
  if (actionType === 'reply_dm' || actionType === 'send_dm') {
    return { domain: 'engagement', worker: 'messages', actionType };
  }
  if (actionType === 'publish_post' || actionType === 'repost_ugc') {
    return { domain: 'content', worker: 'posts', actionType };
  }

  // fallback: treat as content
  return { domain: 'content', worker: 'posts', actionType: actionType || 'publish_post' };
}

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

  // ── Governed read completed → classify content type → dispatch ─────────
  READ_RESULT_AVAILABLE: {
    target: 'EXECUTING',
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
      const table = readDomain === 'db.scheduled-posts' ? 'scheduled_posts' : 'post_queue';
      const rows = data && Array.isArray(data) ? data : [];

      // Classify each row into domain + worker + actionType
      const contentActions = [];
      const engagementActions = [];

      for (const record of rows) {
        const classification = _classify({ table, record });

        if (classification.domain === 'engagement') {
          engagementActions.push({ ...classification, record, accountId });
        } else {
          contentActions.push({ ...classification, record, accountId });
        }
      }

      const actions = [];

      if (contentActions.length > 0) {
        actions.push({
          type: 'EXECUTE_CONTENT',
          accountId,
          items: contentActions.map(a => ({
            worker: a.worker,
            actionType: a.actionType,
            record: a.record,
          })),
        });
      }

      if (engagementActions.length > 0) {
        actions.push({
          type: 'EXECUTE_ENGAGEMENT',
          accountId,
          items: engagementActions.map(a => ({
            worker: a.worker,
            actionType: a.actionType,
            record: a.record,
          })),
        });
      }

      return actions;
    },
  },

  // ── Substrate execution completed → idle ────────────────────────────────
  PUBLISHING_OBSERVATION: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'EXECUTING') {
        return { allowed: false, reason: `Cannot observe publishing from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      if (event.status === 'error') {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'PUBLISH_FAILURE',
          reason: event.metadata?.reason || 'Publishing failed',
        }];
      }
      return [];
    },
  },

  // ── PUBLISH_FAILURE handler REMOVED in Step 7 ──────────────────
  // PUBLISH_FAILURE is observability-only. The emission-orchestrator
  // still emits it (dual emission with WORKER_OUTCOME_REPORTED) for
  // alerting/lineage subscribers. But it does NOT enter a FSM.
  // The canonical terminal-failure transition for publishing-fsm
  // is PUBLISH_RETRY_EXHAUSTED (above), emitted by engagement-fsm
  // when the retry chain is exhausted.

  // ── RETRY_PUBLISH handler REMOVED in Step 7 ─────────────────────
  // This handler was a constitutional bypass — it re-emitted
  // EXECUTE_CONTENT/EXECUTE_ENGAGEMENT from the FSM, bypassing
  // the worker→FSM→classifier→schedule loop. The new architecture
  // has engagement-fsm own retry invocation (via _executeRetry),
  // not publishing-fsm. No code path emits RETRY_PUBLISH.

  // ── PUBLISH_RETRY_EXHAUSTED (Step 7 — terminal failure) ───────────────
  // This is the CANONICAL terminal-failure transition for
  // publishing-fsm. It is emitted by engagement-fsm's
  // _buildExhaustedActions when a publish:* retry chain
  // exhausts (max retries, permanent failure, or sanity
  // rejection). publishing-fsm transitions EXECUTING → IDLE
  // and emits LOG_DEGRADED.
  //
  // REPLACES the previous PUBLISH_FAILURE handler, which was
  // a constitutional bypass (it re-entered the lifecycle
  // from outside the canonical retry cadence loop).
  PUBLISH_RETRY_EXHAUSTED: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'EXECUTING') {
        return { allowed: false, reason: `PUBLISH_RETRY_EXHAUSTED from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      return [{
        type: 'LOG_DEGRADED',
        substate: 'PUBLISH_RETRY_EXHAUSTED',
        reason: event.error || 'Publish retry chain exhausted',
        domain: event.domain,
        retryCount: event.retryCount,
        operation: event.operation,
      }];
    },
  },

  // ── RETRY_IN_PROGRESS (Step 6): engagement-fsm scheduled a retry ─────
  // publishing-fsm stays in EXECUTING while the retry chain is in
  // flight. This gives observability fidelity — the FSM shows that
  // the publish is still in progress, not idle. The next
  // PUBLISHING_OBSERVATION (success or terminal) transitions
  // EXECUTING → IDLE.
  //
  // The event is emitted by engagement-fsm._scheduleRetry when the
  // schedule succeeds for a publish:* domain. CK routes it to
  // publishing-fsm via DOMAIN_EVENT_MAP.
  //
  // The handler is a pure state hold: no actions emitted. The
  // publishing-fsm is transparent to the retry — the actual
  // re-invocation of the publish substrate is driven by
  // engagement-fsm's _executeRetry, not by publishing-fsm.
  RETRY_IN_PROGRESS: {
    target: 'EXECUTING',
    guard: (event) => {
      // Only valid from EXECUTING (the publish is already in flight
      // when the retry schedule happens). If the FSM is in any other
      // state, the event is stale — drop it.
      if (_localState !== 'EXECUTING') {
        return { allowed: false, reason: `RETRY_IN_PROGRESS from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event) => {
      return [];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null;

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch
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
  _lastTransitionedAt = Date.now();

  // Emit observability transition
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
// 5. Initialization
// ═══════════════════════════════════════════════════════════════════════════════

function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[publishing-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Messaging Window Policy
// ═══════════════════════════════════════════════════════════════════════════════

const MESSAGING_WINDOW_HOURS = 24;

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
  return { state: _localState };
}

function getHealth() {
  return { ok: _localState !== 'EXECUTING', signals: { state: _localState } };
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
