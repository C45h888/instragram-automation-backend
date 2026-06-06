// postgres-telemetry-kernel/fsm.js
// Persist-Telemetry FSM: governs all DB write operations.
// Migrated from control-plane/governance/domains/persist-telemetry-fsm.js
//
// Owns: DB write lifecycle (IDLE → WRITING → IDLE), table whitelist
//        validation, backpressure detection, in-flight write tracking.
// Does NOT own: Supabase operations (delegates to db/writers),
//               read operations (delegates to reading-substrate via CK injection).
//
// Any domain needing a DB write MUST route through:
//   domain → CK(DB_WRITE_REQUESTED) → postgres-telemetry-kernel/fsm
//   → db.writers (async worker) → CK(DB_WRITE_COMPLETE)
//   → postgres-telemetry-kernel/fsm → PARSING_COMPLETE + WRITE_ACQUISITION_RESULT
//
// Reports to: constitutional kernel for transition validation + global observability.
//
// Reading-substrate: injected by CK at boot via setReadingSubstrate().
// The FSM does NOT import reading-substrate — CK registers it and injects.
//

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

const db = require('./writers');

// Lazy reading-substrate reference — set by CK after instantiation
let _readingSubstrate = null;

function setReadingSubstrate(substrate) {
  _readingSubstrate = substrate;
}

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
const READ_BACKPRESSURE_THRESHOLD = 20;

// ═══════════════════════════════════════════════════════════════════════════════
// 0b. Read Domain Whitelist — which callers are permitted to read which domains
// ═══════════════════════════════════════════════════════════════════════════════

const READ_DOMAIN_WHITELIST = new Set([
  'db.media',
  'db.post-queue',
  'db.accounts',
  'db.scheduled-posts',
  'ig.content',
  'ig.engagement',
  'ig.insights',
  'ig.ugc',
]);

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No writes or reads in flight',
  },
  WRITING: {
    description: 'One or more writes in progress',
  },
  READ_EXECUTING: {
    description: 'One or more governed reads in progress',
  },
  BACKPRESSURE: {
    description: 'Write queue exceeds threshold — degradation',
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {
  DB_READ_OBSERVED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async () => [],
    // Pure telemetry — no gate, no state change.
    // DB readers emit this fire-and-forget on every read for observability.
  },

  // ── Governed Reads ──────────────────────────────────────────────────────
  DB_READ_REQUESTED: {
    target: () => {
      if (_readsInFlight + 1 > READ_BACKPRESSURE_THRESHOLD) {
        return _localState === 'IDLE' ? 'READ_EXECUTING' : _localState;
      }
      return _localState === 'IDLE' ? 'READ_EXECUTING' : _localState;
    },
    guard: (event) => {
      const { readDomain } = event;
      if (!readDomain || !READ_DOMAIN_WHITELIST.has(readDomain)) {
        return { allowed: false, reason: `read domain not whitelisted: ${readDomain}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { readDomain, accountId, readId, params } = event;

      // Track in-flight
      _readsInFlight++;

      // Delegate to reading-substrate — async, non-blocking
      // _readingSubstrate is injected by CK via setReadingSubstrate() at boot
      if (_readingSubstrate) {
        const readPromise = _readingSubstrate.executeRead(readDomain, params, readId);
        // Fire completion back through governance when done
        readPromise.then((result) => {
          if (ctx && ctx.dispatchGlobal) {
            ctx.dispatchGlobal({
              type: 'DB_READ_COMPLETE',
              readDomain,
              accountId,
              readId,
              success: result.success,
              data: result.data,
              error: result.error,
              latencyMs: result.latencyMs,
              cached: result.cached || false,
            });
          }
        }).catch((err) => {
          if (ctx && ctx.dispatchGlobal) {
            ctx.dispatchGlobal({
              type: 'DB_READ_COMPLETE',
              readDomain,
              accountId,
              readId,
              success: false,
              data: null,
              error: err.message,
              latencyMs: 0,
            });
          }
        });
      } else {
        // No reading substrate wired — fail fast
        _readsInFlight = Math.max(0, _readsInFlight - 1);
        return [{
          type: 'DB_READ_COMPLETE',
          readDomain,
          accountId,
          readId,
          success: false,
          error: 'reading_substrate_unavailable',
          latencyMs: 0,
        }];
      }

      if (_readsInFlight > READ_BACKPRESSURE_THRESHOLD) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'READ_BACKPRESSURE',
          reason: `Read queue at ${_readsInFlight} (threshold ${READ_BACKPRESSURE_THRESHOLD})`,
        }];
      }

      return [];
    },
  },

  DB_READ_COMPLETE: {
    target: () => {
      if (_readsInFlight === 0 && _inFlight === 0) return 'IDLE';
      if (_readsInFlight > 0) return 'READ_EXECUTING';
      return _localState;
    },
    guard: () => {
      if (_readsInFlight <= 0) {
        return { allowed: false, reason: 'No reads in flight — cannot complete read' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { readDomain, accountId, readId, success, data, error, latencyMs } = event;
      _readsInFlight = Math.max(0, _readsInFlight - 1);

      if (error) {
        return [{
          type: 'LOG_DEGRADED',
          substate: 'READ_FAILURE',
          reason: `Read failed for ${readDomain}/${accountId}: ${error}`,
        }];
      }

      // Forward read result to calling domain
      const actions = [];
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'READ_RESULT_AVAILABLE',
          accountId,
          readDomain,
          readId,
          data,
          latencyMs,
        });
      }

      return actions;
    },
  },

  // ── Writes ──────────────────────────────────────────────────────────────

  DB_WRITE_REQUESTED: {
    target: () => {
      if (_inFlight + 1 > BACKPRESSURE_THRESHOLD) return 'BACKPRESSURE';
      return _localState === 'IDLE' ? 'WRITING' : _localState;
    },
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const { domain, accountId, intentId, table, operation, rows } = event;

      // Layer 1: table whitelist (existing — hard reject)
      if (!VALID_TABLES.has(table)) {
        return [{
          type: 'DB_WRITE_COMPLETE',
          domain, accountId, intentId,
          table, count: 0,
          status: 'failed',  // Step 5 fix: caller checks status === 'failed'
          error: `rejected_table: ${table}`,
          authority: 'persist-telemetry-fsm',
        }];
      }

      // Layer 2: sanity gate (universal gate — defense-in-depth)
      // Gating the DB write itself. The db.writers substrate
      // is NOT called. GATE_REJECTED preserves telemetry.
      // DB_WRITE_COMPLETE(status: failed) preserves chain
      // integrity (the caller's PARSING_COMPLETE handler
      // sees the failure).
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'db_write',
        accountId,
        table,
        intentId,
        rowCount: rows?.length || 0,
      });
      if (!gate.allowed) {
        // Track as in-flight so DB_WRITE_COMPLETE (the
        // self-completion we emit) can transition the
        // FSM back to IDLE. The _inFlight++ and
        // DB_WRITE_COMPLETE pair matches the normal
        // db.dispatchWrite path (which increments
        // _inFlight and gets a real completion later).
        _inFlight++;
        return [
          {
            type: 'GATE_REJECTED',
            operation: 'db_write',
            accountId,
            table,
            intentId,
            reason: gate.reason || 'gate_rejected',
          },
          {
            type: 'DB_WRITE_COMPLETE',
            domain, accountId, intentId,
            table, count: 0,
            status: 'failed',
            error: `gate_rejected: ${gate.reason || 'gate_rejected'}`,
            authority: 'persist-telemetry-fsm',
          },
        ];
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
    buildActions: async (event, ctx) => {
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
let _inFlight = 0;        // writes in flight
let _readsInFlight = 0;   // reads in flight

// ── Default fail-open sanity check (universal gate pattern) ─────────────
// The ctx.sanityCheck is the universal gate. The FSM calls it
// during emission. For tests / non-CK dispatch, the default is
// always-allowed (fail-open to preserve operational cadence).
const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch
// ═══════════════════════════════════════════════════════════════════════════════

async function dispatch(event, ctx) {
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

  const actions = (txn.buildActions ? await txn.buildActions(event, ctx) : []);

  console.log(`[persist-telemetry-fsm] ${from} → ${target}  (${event.type})`);

  return { allowed: true, from, to: target, actions };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Initialization — called by constitutional kernel on boot with rehydrated state
// ═══════════════════════════════════════════════════════════════════════════════

function init(rehydratedState) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() { return _localState; }
function exportState() { return { state: _localState, writesInFlight: _inFlight, readsInFlight: _readsInFlight }; }
function getHealth() {
  return {
    ok: _inFlight < BACKPRESSURE_THRESHOLD && _readsInFlight < READ_BACKPRESSURE_THRESHOLD,
    signals: { writesInFlight: _inFlight, readsInFlight: _readsInFlight },
  };
}

module.exports = {
  name: 'persist-telemetry',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  setReadingSubstrate,
};