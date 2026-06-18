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
const crypto = require('crypto');
// analyzeFailure is called by the FSM itself (constitutional pattern)
// when workers emit raw errors. Workers no longer import the substrate.
const { analyzeFailure } = require('./substrates/persistence-failure-substrate');

// Lazy reading-substrate reference — set by CK after instantiation
let _readingSubstrate = null;

function setReadingSubstrate(substrate) {
  _readingSubstrate = substrate;
}

// ═══════════════════════════════════════════════════════════════════════════════

// ── Governance reference (set by CK at boot) ────────────────────────────
// The FSM holds a governance ref for worker invocation and event dispatch.
// engagement-fsm pattern: set at boot, passed through execution contexts.
let _governance = null;

function setGovernance(governance) {
  if (governance && typeof governance.dispatch === 'function') {
    _governance = governance;
  }
}

function getGovernance() {
  return _governance;
}

// ── Worker registry (local) ───────────────────────────────────────────
// Each FSM holds its own worker map. CK registration happens at boot
// via constitutional.registerWorker(fsmName, workerName, worker).
// The CTX gate (ctx.invokeWorker) validates ownership through CK.
const _workers = new Map();

function registerWorker(name, worker) {
  _workers.set(name, worker);
}

function getWorker(name) {
  return _workers.get(name) || null;
}

function getWorkers() {
  return _workers;
}


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
  'instagram_credentials',
  'instagram_business_accounts',
  'token_lifecycle_events',
  'system_alerts',
  'user_profiles',
  'api_usage',
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
  'db.scope-cache',
  'db.credential',
  'db.encryption-key',
  'db.alerts',
  'db.lifecycle-events',
  'db.user-profiles',
  'db.api-usage',
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

      // Store params for retry path (read cadence loop)
      _pendingReads.set(readId, { readDomain, accountId, params });

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
      // Evaluate AFTER decrement: _readsInFlight is decremented in
      // buildActions (line 248). If the current value is 1, after
      // decrement it becomes 0 → transition to IDLE. If > 1, stay
      // in READ_EXECUTING for remaining reads. (Phase 7, B-NEW-5)
      const afterDecrement = Math.max(0, _readsInFlight - 1);
      if (afterDecrement === 0 && _inFlight === 0) return 'IDLE';
      if (afterDecrement > 0) return 'READ_EXECUTING';
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
        // Look up original read params from _pendingReads so the retry
        // worker can reconstruct the read query.
        const pending = _pendingReads.get(readId);
        _pendingReads.delete(readId);
        const readParams = pending?.params || null;

        const errorShape = analyzeFailure({ message: error }, 'read', 'supabase', { attemptN: 1 });
        const actions = [{
          type: 'LOG_DEGRADED',
          substate: 'READ_FAILURE',
          reason: `Read failed for ${readDomain}/${accountId}: ${error}`,
        }, {
          type: 'DB_READ_FAILED',
          readDomain, accountId, readId,
          errorShape, error, latencyMs,
          readParams,  // original query params for retry
        }];
        if (ctx && ctx.dispatchGlobal) {
          ctx.dispatchGlobal({
            type: 'DB_PERSIST_FAILURE_READ',
            readDomain, accountId, readId,
            errorShape, error, latencyMs,
            readParams,  // forward to retry-cadence FSM
          });
        }
        return actions;
      }
      // Forward read result to calling domain. Attach lineageId+lineageDomain
      // to satisfy the canonical-source gate. The lineageDomain names
      // persist-telemetry as the issuer (the executor of the read);
      // the gate allows this because persist-telemetry is a registered
      // constitutional citizen. (Phase 7 Findings, B1)
      const actions = [];
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'READ_RESULT_AVAILABLE',
          accountId,
          readDomain,
          readId,
          data,
          latencyMs,
          lineageId: `persist-telemetry-read-complete-${readId}`,
          lineageDomain: 'persist-telemetry',
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

      // Step 6: idempotency key generation for state-mutating writes.
      // Hash of (lineageId + table + pkField + pkValue) per Q3.
      // Same retry of the same intent produces the same key.
      const idempotencyKey = _generateIdempotencyKey(accountId, intentId, table, rows);

      // Delegate to db writers substrate — async, non-blocking
      db.dispatchWrite(operation, { domain, accountId, intentId, table, rows, idempotencyKey });

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

      // Success confirmation span: the constitutional path for retry-cadence
      // is the FAILURE path (DB_PERSIST_FAILURE on DB_WRITE_FAILED). The
      // success path closes the in-flight counter and emits a span so the
      // observability plane sees the write landed. No retry is instantiated
      // — the retry-cadence kernel only acts on failures.
      if (ctx && ctx.dispatchGlobal) {
        try {
          ctx.dispatchGlobal({
            type: 'DB_WRITE_CONFIRMED',
            domain, accountId, intentId, table,
            count: count || 0,
            writeId: event.writeId || null,
            lineageId: event.lineageId || null,
          });
        } catch (_) { /* observation is best-effort; do not block the path */ }
      }

      // Forward completion to originating domain so it can resolve pending writes.
      // DB_WRITE_ACKNOWLEDGED is routed by CK to the domain that dispatched DB_WRITE_REQUESTED.
      // This closes the request-and-await loop for credential status updates and other
      // operational writes where the caller must confirm the write landed.
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
        // Forward acknowledgement to originating domain — graph-capability FSM
        // resolves pendingWrites Promise, unblocking the awaiting worker.
        ctx.dispatchGlobal({
          type: 'DB_WRITE_ACKNOWLEDGED',
          domain, accountId, table,
          writeId: event.writeId || `${domain}:${table}:${accountId}:${Date.now()}`,
          success: !error,
          error: error || null,
        });
      }

      return actions;
    },
  },

  // ── DB_WRITE_FAILED — worker emitted a raw failure ───────────────────
  // Constitutional path: the FSM is the sole caller of the persistence-
  // failure substrate. Workers emit raw errors only (no pre-classification).
  // The FSM calls analyzeFailure to produce the canonical analysis, then
  // evaluates severity and forwards to DB_PERSIST_FAILURE.
  // For CRITICAL severity, a CRITICAL_FAILURE_OBSERVED action is emitted
  // immediately (bypasses normal flow per spec §9).
  //
  // Legacy modes (analysis/errorShape) are accepted for backwards compat
  // with any unmigrated callers.
  DB_WRITE_FAILED: {
    target: () => {
      if (_inFlight === 0) return 'IDLE';
      if (_inFlight > BACKPRESSURE_THRESHOLD) return 'BACKPRESSURE';
      return _localState;
    },
    guard: (event) => {
      // Accept rawError (constitutional), analysis (canonical), or errorShape (legacy)
      if (!event || (!event.rawError && !event.analysis && !event.errorShape)) {
        return { allowed: false, reason: 'DB_WRITE_FAILED requires rawError, analysis, or errorShape' };
      }
      if (_inFlight <= 0) {
        return { allowed: false, reason: 'No writes in flight — cannot fail write' };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { domain, accountId, intentId, table, count, analysis, errorShape, error, rawError } = event;
      let canonical = analysis || null;

      // CONSTITUTIONAL PATH: FSM calls substrate when worker emits raw error
      if (!canonical && rawError) {
        const { workerName, lineageId, primaryKeyField, primaryKeyValue, attemptN, operation, source } = event;
        canonical = analyzeFailure(
          rawError,
          operation || 'write',
          source || 'supabase',
          { attemptN: attemptN || 1, lineageId, workerName, primaryKeyField, primaryKeyValue }
        );
      }

      // analysis is the canonical shape; errorShape is the slim legacy wrapper.
      // If only errorShape is present (backwards compat), use its fields.
      const effectiveCategory = canonical?.category || errorShape?.category || 'UNKNOWN';
      const effectiveSubtype = canonical?.subtype || errorShape?.subtype || 'unknown';
      const effectiveRetryable = canonical?.retryable ?? errorShape?.retryable ?? false;
      const effectiveSeverity = canonical?.severity || 'MEDIUM';
      const effectiveIdempotencyKey = canonical?.idempotencyKey || null;
      _inFlight = Math.max(0, _inFlight - 1);

      const actions = [{
        type: 'LOG_DEGRADED',
        substate: 'WRITE_FAILURE',
        reason: `DB write failed for ${domain}/${table}: category=${effectiveCategory} subtype=${effectiveSubtype} retryable=${effectiveRetryable} severity=${effectiveSeverity}`,
      }];

      // CRITICAL severity bypasses normal flow (Q2)
      if (effectiveSeverity === 'CRITICAL') {
        actions.push({
          type: 'CRITICAL_FAILURE_OBSERVED',
          domain, accountId, intentId, table,
          category: effectiveCategory, subtype: effectiveSubtype,
          severity: 'CRITICAL', analysis: canonical,
        });
      }
      // HIGH severity fires observer but continues normal flow
      if (effectiveSeverity === 'HIGH') {
        actions.push({
          type: 'HIGH_FAILURE_OBSERVED',
          domain, accountId, intentId, table,
          category: effectiveCategory, subtype: effectiveSubtype,
          severity: 'HIGH', analysis: canonical,
        });
      }

      // Forward full analysis to retry-cadence via the constitutional flow
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'DB_PERSIST_FAILURE',
          domain, accountId, intentId, table,
          operation: event.operation || null,
          rows: event.rows || null,
          analysis: canonical || null,
          // backwards compat — keep errorShape so the retry-cadence FSM
          // can fall back if analysis is null
          errorShape: errorShape || { category: effectiveCategory, subtype: effectiveSubtype, retryable: effectiveRetryable },
          error,
          idempotencyKey: effectiveIdempotencyKey,
        });
      }

      return actions;
    },
  },

  // ── CRITICAL_FAILURE_OBSERVED — severity CRITICAL bypass ────────────────
  // Per spec §9 / Q2: CRITICAL severity fires immediately regardless of
  // the normal recommendation flow. The observer event is the action;
  // the normal DB_WRITE_FAILED path continues independently.
  CRITICAL_FAILURE_OBSERVED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'LOG_CRITICAL',
      substate: 'CRITICAL_FAILURE',
      reason: `CRITICAL: ${event.category}/${event.subtype} on ${event.table || 'unknown'}`,
      severity: 'CRITICAL',
    }],
  },

  // ── HIGH_FAILURE_OBSERVED — severity HIGH observer ──────────────────────
  HIGH_FAILURE_OBSERVED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => [{
      type: 'LOG_DEGRADED',
      substate: 'HIGH_FAILURE',
      reason: `HIGH: ${event.category}/${event.subtype} on ${event.table || 'unknown'}`,
      severity: 'HIGH',
    }],
  },

  // ── DB_READ_FAILED — reader emitted a structured failure ───────────────
  DB_READ_FAILED: {
    target: () => {
      if (_readsInFlight === 0 && _inFlight === 0) return 'IDLE';
      if (_readsInFlight > 0) return 'READ_EXECUTING';
      return _localState;
    },
    guard: (event) => {
      if (!event || (!event.analysis && !event.errorShape)) {
        return { allowed: false, reason: 'DB_READ_FAILED requires analysis or errorShape' };
      }
      if (_readsInFlight <= 0) {
        return { allowed: false, reason: 'No reads in flight — cannot fail read' };
      }
      return { allowed: true };
    },
    buildActions: async (event) => {
      const { readDomain, accountId, readId, analysis, errorShape, error, latencyMs } = event;
      const effectiveCategory = analysis?.category || errorShape?.category || 'UNKNOWN';
      const effectiveSubtype = analysis?.subtype || errorShape?.subtype || 'unknown';
      const effectiveRetryable = analysis?.retryable ?? errorShape?.retryable ?? false;
      const effectiveSeverity = analysis?.severity || 'MEDIUM';
      _readsInFlight = Math.max(0, _readsInFlight - 1);

      const actions = [{
        type: 'LOG_DEGRADED',
        substate: 'READ_FAILURE',
        reason: `DB read failed for ${readDomain}/${accountId}: category=${effectiveCategory} subtype=${effectiveSubtype} retryable=${effectiveRetryable} severity=${effectiveSeverity}`,
      }];

      if (effectiveSeverity === 'CRITICAL') {
        actions.push({
          type: 'CRITICAL_FAILURE_OBSERVED',
          readDomain, accountId, readId,
          category: effectiveCategory, subtype: effectiveSubtype,
          severity: 'CRITICAL', analysis,
        });
      }

      return actions;
    },
  },

  // ── WORKER_RESULT — observability-only (no workers registered) ──────────
  // Persist-telemetry FSM does not register or invoke workers through
  // ctx.invokeWorker(). DB writes and reads are handled directly through
  // CK's GLOBAL_TRANSITION_MAP. This handler exists for completeness.
  WORKER_RESULT: {
    target: null,
    guard: () => ({ allowed: true }),
    buildActions: () => [],
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state
// ═══════════════════════════════════════════════════════════════════════════════

// ── Idempotency key generator (Step 6) ────────────────────────────────────
// Hash of (lineageId + table + pkField + pkValue) per Q3.
// Same retry of the same intent → same key.
// Picks the primary key from the first row of the batch based on
// the table's known conflict key.
const TABLE_PK_MAP = {
  instagram_comments:            'instagram_comment_id',
  instagram_dm_messages:          'instagram_message_id',
  instagram_dm_conversations:     'instagram_thread_id',
  instagram_media:                'instagram_media_id',
  ugc_content:                    'business_account_id',
  api_usage:                      'user_id',
  system_alerts:                  'business_account_id',
  token_lifecycle_events:         'credential_id',
  instagram_credentials:          'user_id',
  instagram_business_accounts:    'user_id',
};

function _generateIdempotencyKey(accountId, intentId, table, rows) {
  if (!intentId) return null;
  const pkField = TABLE_PK_MAP[table] || null;
  if (!pkField) return null;
  const firstRow = (rows && rows[0]) || {};
  const pkValue = firstRow[pkField] ?? null;
  if (pkValue == null) return null;
  const input = `${accountId || '*'}|${intentId}|${table}|${pkField}|${pkValue}`;
  return crypto.createHash('sha256').update(input).digest('hex').slice(0, 32);
}

let _localState = 'IDLE';
let _lastTransitionedAt = null;
let _inFlight = 0;        // writes in flight
let _readsInFlight = 0;   // reads in flight

// ── Pending read params: readId → { readDomain, accountId, params } ──────
// Stored at DB_READ_REQUESTED so the retry path can reconstruct the
// original read query when a read fails and needs retrying.
const _pendingReads = new Map();

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

async function _syncProjectionState() {
  try {
    const { getRedisClient } = require('../config/redis');
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      const raw = await redis.get('lineage:projection:domain:persist-telemetry');
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && parsed.projection && parsed.projection.state) {
          if (typeof _localState !== 'undefined') {
            _localState = parsed.projection.state;
          }
        }
      }
    }
  } catch (_) {}
}

async function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  await _syncProjectionState();

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
  setGovernance,
  getGovernance,
  registerWorker,
  getWorker,
  getWorkers,
  name: 'persist-telemetry',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  setReadingSubstrate,
};
