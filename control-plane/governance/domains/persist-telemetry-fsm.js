// control-plane/governance/domains/persist-telemetry-fsm.js
// Persist-Telemetry FSM: governs all DB write operations.
//
// Owns: DB write lifecycle (IDLE → WRITING → IDLE), table whitelist
//        validation, backpressure detection, in-flight write tracking.
// Does NOT own: Supabase operations (delegates to db/writers),
//               read operations (CK handles directly), parse/normalize.
//
// Any domain needing a DB write MUST route through:
//   domain → CK(DB_WRITE_REQUESTED) → persist-telemetry-fsm
//   → db.writers (async worker) → CK(DB_WRITE_COMPLETE)
//   → persist-telemetry-fsm → PARSING_COMPLETE + WRITE_ACQUISITION_RESULT
//
// Reports to: constitutional kernel for transition validation + global observability.

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

const db = require('../../../substrates/db/writers');

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Governance Policy Constants
// ═══════════════════════════════════════════════════════════════════════════════

const VALID_TABLES = new Set([
  'instagram_comments',
  'instagram_dm_messages',
  'instagram_dm_conversations',
  'instagram_media',
  'ugc_content',
  'post_queue',
  'scheduled_posts',
  'ugc_permissions',
]);

const BACKPRESSURE_THRESHOLD = 10;

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No writes in flight',
  },
  WRITING: {
    description: 'One or more writes in progress',
  },
  BACKPRESSURE: {
    description: 'Write queue exceeds threshold — degradation',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  DB_WRITE_REQUESTED: {
    target: () => {
      if (_inFlight + 1 > BACKPRESSURE_THRESHOLD) return 'BACKPRESSURE';
      return _localState === 'IDLE' ? 'WRITING' : _localState;
    },
    guard: () => ({ allowed: true }),
    buildActions: (event, ctx) => {
      const { domain, accountId, intentId, table, operation, rows } = event;

      // Gate: validate table whitelist
      if (!VALID_TABLES.has(table)) {
        return [{
          type: 'DB_WRITE_COMPLETE',
          domain, accountId, intentId,
          table, count: 0,
          error: `rejected_table: ${table}`,
          authority: 'persist-telemetry-fsm',
        }];
      }

      // Track in-flight
      _inFlight++;

      // Delegate to db writers substrate — async, non-blocking
      db.dispatchWrite(operation, { domain, accountId, intentId, table, rows });

      if (_inFlight > BACKPRESSURE_THRESHOLD) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'BACKPRESSURE',
          reason: `Write queue at ${_inFlight} (threshold ${BACKPRESSURE_THRESHOLD})`,
        }];
      }

      return [];
    },
  },

  DB_WRITE_COMPLETE: {
    target: () => {
      if (_inFlight === 0) return 'IDLE';
      if (_inFlight > BACKPRESSURE_THRESHOLD) return 'BACKPRESSURE';
      return _localState; // stay WRITING if more in flight
    },
    guard: () => {
      if (_localState !== 'WRITING' && _localState !== 'BACKPRESSURE') {
        return { allowed: false, reason: `Cannot complete write from ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const { domain, accountId, intentId, table, count, error } = event;
      _inFlight = Math.max(0, _inFlight - 1);

      if (error) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'PARTIAL_FAILURE',
          reason: `DB write failed for ${domain}/${table}: ${error}`,
        }];
      }

      // Forward completion to acquisition domain
      const actions = [];
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'PARSING_COMPLETE',
          accountId, domain, intentId,
          result: { status: 'completed', count },
        });
        ctx.dispatchGlobal({
          type: 'WRITE_ACQUISITION_RESULT',
          accountId, domain, intentId,
          result: { status: 'completed', count },
        });
      }

      return actions;
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null;
let _inFlight = 0;

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
    const result = txn.guard(event, ctx);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  const rawTarget = txn.target;
  const target = typeof rawTarget === 'function' ? rawTarget(event, ctx) : rawTarget;

  if (target === null) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  if (ctx && ctx.validate) {
    const validation = ctx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  _localState = target;
  _lastTransitionedAt = Date.now();

  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'persist-telemetry',
        entity: 'fsm',
        entityId: 'persist-telemetry-fsm',
        previousState: from,
        nextState: target,
        authority: 'persist-telemetry-fsm',
        raw: { intent: event.type, table: event.table || null, inFlight: _inFlight },
      });
    }
  } catch (_) {}

  const actions = txn.buildActions ? txn.buildActions(event, ctx) : [];

  console.log(`[persist-telemetry-fsm] ${from} → ${target}  (${event.type})`);

  return { allowed: true, from, to: target, actions };
}

function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
  }
}

function getState() { return _localState; }
function exportState() { return { state: _localState, inFlight: _inFlight }; }
function getHealth() { return { ok: _inFlight < BACKPRESSURE_THRESHOLD, signals: { inFlight: _inFlight } }; }

module.exports = {
  name: 'persist-telemetry',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
};
