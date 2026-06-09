// scheduling-kernel/fsm.js
// Scheduling Domain FSM: federated state machine governing maintenance cadence.
// Kernelized from: control-plane/governance/domains/scheduling-fsm.js
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
//   Lineage     → ctx.recordLineage() writes to authoritative ledger (via CK mediation)
//
// Domain FSMs CANNOT directly access the lineage ledger.
// The constitutional kernel mediates all lineage writes.
//
// Local states:
//   IDLE       — between cadence cycles
//   SCANNING   — scanning database for publishable items
//   REFRESHING — refreshing account lifecycle
//   CHECKING   — running safety checks and metrics

// Lazy import to avoid circular dependency
let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

const crypto = require('crypto');

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
  // ── Cadence tick → SCANNING: FSM-owned governed read + lifecycle worker ──
  // The FSM is locally reactive: if stuck in a non-IDLE state from a prior
  // failed cycle, it self-recovers by resetting to SCANNING and retrying.
  // The FSM owns the governed read — it fetches accounts through CK, then
  // passes them to the lifecycle-refresh worker. The worker is a pure executor;
  // the FSM makes all governance decisions.
  //
  // Stuck-state recovery: if CADENCE_TICK arrives while FSM is still in
  // SCANNING/REFRESHING/CHECKING from a prior failed attempt, the FSM logs
  // the anomaly and retries from SCANNING. The cadence timer (90s) is the
  // recovery mechanism — next tick always resets and retries.
  CADENCE_TICK: {
    target: (event) => {
      if (_localState !== 'IDLE') {
        console.warn(`[scheduling-fsm] Stuck in ${_localState} — self-recovering to SCANNING`);
      }
      _lastCycleStartedAt = Date.now();
      return 'SCANNING';
    },
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const gate = await _resolveSanityCheck(ctx, {
        operation: 'cadence_tick',
        domain: 'scheduling',
      });
      if (!gate.allowed) return [];

      // ── FSM-owned governed read — fetch active accounts through CK ──────
      let accounts = [];
      const gov = getGovernance();
      if (gov && typeof gov.governedRead === 'function') {
        try {
          const readResult = await gov.governedRead('db.accounts', { query: 'getActiveAccounts' });
          accounts = readResult.success ? readResult.data : [];
        } catch (err) {
          console.error('[scheduling-fsm] Governed read failed:', err.message);
          return [{ type: 'LOG_DEGRADED', substate: 'GOVERNED_READ_FAILURE', reason: err.message }];
        }
      }

      // ── Invoke worker with account data ────────────────────────────────
      let workerResult = null;
      try {
        workerResult = await ctx.invokeWorker('lifecycle-refresh', { accounts });
      } catch (err) {
        console.error('[scheduling-fsm] lifecycle-refresh worker failed:', err.message);
        return [{ type: 'LOG_DEGRADED', substate: 'LIFECYCLE_FAILURE', reason: err.message }];
      }

      // ── Emit per-account lifecycle events + aggregate refresh event ────
      const actions = [];
      const added = workerResult ? workerResult.added : [];
      const removed = workerResult ? workerResult.removed : [];
      const currentIds = workerResult ? workerResult.currentIds : [];

      for (const id of added) {
        actions.push({ type: 'ACCOUNT_ADDED', accountId: id });
      }
      for (const id of removed) {
        actions.push({ type: 'ACCOUNT_REMOVED', accountId: id });
      }
      actions.push({
        type: 'LIFECYCLE_REFRESHED',
        accountIds: currentIds,
        added,
        removed,
      });

      return actions;
    },
  },

  // ── Lifecycle refreshed → REFRESHING: invoke safety-check worker ────────
  // The orchestrator fans LIFECYCLE_REFRESHED back into CK, which routes
  // it here. FSM transitions SCANNING → REFRESHING, invokes safety-check
  // worker, then emits SAFETY_CHECK_COMPLETE for the next step.
  LIFECYCLE_REFRESHED: {
    target: 'REFRESHING',
    guard: (event) => {
      if (_localState !== 'SCANNING') {
        return { allowed: false, reason: `LIFECYCLE_REFRESHED only valid from SCANNING, got ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      // Store accounts from lifecycle refresh for later emission
      if (event.accountIds && event.accountIds.length > 0) {
        _lastLifecycleAccountIds = event.accountIds;
        // Also emit UPDATE_ACCOUNTS so the membrane can update CK's account list
        try {
          await ctx.invokeWorker('safety-check', {});
        } catch (err) {
          console.error('[scheduling-fsm] safety-check worker failed:', err.message);
          return [{ type: 'LOG_DEGRADED', substate: 'SAFETY_FAILURE', reason: err.message }];
        }
        return [{ type: 'SAFETY_CHECK_COMPLETE' }];
      }
      // No accounts — skip safety check, go straight to metrics
      return [{ type: 'SAFETY_CHECK_COMPLETE' }];
    },
  },

  SAFETY_CHECK_COMPLETE: {
    target: 'CHECKING',
    guard: (event) => {
      if (_localState !== 'REFRESHING') {
        return { allowed: false, reason: `SAFETY_CHECK_COMPLETE only valid from REFRESHING, got ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      let metricsResult = null;
      try {
        metricsResult = await ctx.invokeWorker('metrics-report', {});
      } catch (err) {
        console.error('[scheduling-fsm] metrics-report worker failed:', err.message);
        return [{ type: 'LOG_DEGRADED', substate: 'METRICS_FAILURE', reason: err.message }];
      }

      if (metricsResult && metricsResult.signals) {
        return [{
          type: 'WORKER_METRICS_REPORTED',
          total: metricsResult.signals.total,
          failed: metricsResult.signals.failed,
          failureRate: metricsResult.signals.failureRate,
          windowMs: metricsResult.signals.windowMs,
        }];
      }
      return [{ type: 'WORKER_METRICS_REPORTED', total: 0, failed: 0, failureRate: 0, windowMs: 0 }];
    },
  },

  // ── Cadence lifecycle events — emitted by cadence substrate via CK ────
  CADENCE_LOOP_STARTED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      console.log(`[scheduling-fsm] Cadence loop started — interval ${event.intervalMs}ms`);
      return [];
    },
  },
  CADENCE_LOOP_STOPPED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      console.log(`[scheduling-fsm] Cadence loop stopped`);
      return [];
    },
  },
  CADENCE_CYCLE_COMPLETED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      _lastCadenceCycleCompletedAt = event.tickAt || Date.now();
      return [];
    },
  },
  CADENCE_CYCLE_FAILED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      console.error(`[scheduling-fsm] Cadence cycle failed: ${event.error || 'unknown'}`);
      return [{ type: 'LOG_DEGRADED', substate: 'CADENCE_FAILURE', reason: event.error || 'cadence cycle failed' }];
    },
  },

  // ── Metrics record → buffer (FAST PATH) ─────────────────────────────────
  // Routed by CK.recordMetric(). The FSM buffers the record instead of
  // writing immediately. The buffer is drained on METRICS_FLUSH (cadence-driven).
  // Callers get immediate acknowledgment — the FSM owns when writes commit.
  METRICS_RECORD_REQUESTED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: (event) => {
      _writeBuffer.push({
        domain: event.domain,
        status: event.status,
        latencyMs: event.latencyMs,
        accountId: event.accountId || null,
        metricId: event.metricId,
        ts: Date.now(),
      });
      return [{
        type: 'METRICS_RECORD_ACCEPTED',
        metricId: event.metricId,
        bufferSize: _writeBuffer.length,
      }];
    },
  },

  // ── Metrics query → cache (FAST PATH) ───────────────────────────────────
  // Routed by CK.queryMetrics(). The FSM checks the read cache first.
  // Cache hit → return cached data immediately. Cache miss → invoke
  // metrics-query-worker and cache the result. Cache is invalidated
  // on METRICS_FLUSH (new writes change the data).
  METRICS_QUERY_REQUESTED: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const cacheKey = `query:${event.queryType}:${JSON.stringify(event.params || {})}`;
      const cached = _readCache.get(cacheKey);

      if (cached) {
        return [
          { type: 'METRICS_CACHE_HIT', queryId: event.queryId, queryType: event.queryType },
          { type: 'METRICS_QUERY_COMPLETE', queryId: event.queryId, data: cached.data },
        ];
      }

      // Cache miss — invoke worker
      try {
        const result = await ctx.invokeWorker('metrics-query', {
          queryType: event.queryType,
          params: event.params,
        });
        _readCache.set(cacheKey, { data: result.data, cachedAt: Date.now() });
        return [
          { type: 'METRICS_CACHE_MISS', queryId: event.queryId, queryType: event.queryType },
          { type: 'METRICS_QUERY_COMPLETE', queryId: event.queryId, data: result.data },
        ];
      } catch (err) {
        console.error('[scheduling-fsm] metrics-query worker failed:', err.message);
        return [{
          type: 'METRICS_QUERY_COMPLETE',
          queryId: event.queryId,
          data: null,
          error: err.message,
        }];
      }
    },
  },

  // ── Metrics flush → drain buffer (CADENCE-DRIVEN) ────────────────────────
  // Fired at the end of each CADENCE_TICK cycle (CHECKING → FLUSHING → IDLE).
  // Drains the write buffer, invokes metrics-flush-worker for batch write,
  // clears the read cache (fresh data after write), updates _lastFlushAt.
  METRICS_FLUSH: {
    target: () => _localState,
    guard: () => ({ allowed: true }),
    buildActions: async (event, ctx) => {
      const count = _writeBuffer.length;
      if (count === 0) {
        return [{ type: 'METRICS_BUFFER_DRAINED', count: 0 }];
      }

      // Snapshot and clear buffer (atomic — new writes during flush go to fresh buffer)
      const batch = _writeBuffer.splice(0);
      _readCache.clear(); // invalidate cache — data is stale after write

      try {
        const result = await ctx.invokeWorker('metrics-flush', { records: batch });
        _lastFlushAt = Date.now();
        return [{
          type: 'METRICS_BUFFER_DRAINED',
          count: result.count || 0,
          errors: result.errors || null,
        }];
      } catch (err) {
        console.error('[scheduling-fsm] metrics-flush worker failed:', err.message);
        // Re-queue: put records back at front of buffer for next flush
        _writeBuffer.unshift(...batch);
        return [{
          type: 'METRICS_FLUSH_FAILED',
          count,
          error: err.message,
        }];
      }
    },
  },

  // ── Worker metrics → IDLE: drain metrics buffer, emit domain list ────
  // The orchestrator fans WORKER_METRICS_REPORTED back into CK, which
  // routes it here. FSM transitions CHECKING → IDLE. Before returning to
  // IDLE, the FSM drains the metrics write buffer via METRICS_FLUSH.
  // This ensures all buffered writes from the cycle are committed.
  // Emits UPDATE_DOMAIN_LIST and UPDATE_ACCOUNTS to close the cycle.
  WORKER_METRICS_REPORTED: {
    target: 'IDLE',
    guard: (event) => {
      if (_localState !== 'CHECKING') {
        return { allowed: false, reason: `WORKER_METRICS_REPORTED only valid from CHECKING, got ${_localState}` };
      }
      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const actions = [];

      // ── Drain metrics write buffer (cadence-driven commit) ──────────
      const count = _writeBuffer.length;
      if (count > 0) {
        const batch = _writeBuffer.splice(0);
        _readCache.clear();
        try {
          await ctx.invokeWorker('metrics-flush', { records: batch });
          _lastFlushAt = Date.now();
          actions.push({ type: 'METRICS_BUFFER_DRAINED', count });
        } catch (err) {
          console.error('[scheduling-fsm] metrics-flush failed:', err.message);
          _writeBuffer.unshift(...batch);
          actions.push({ type: 'METRICS_FLUSH_FAILED', count, error: err.message });
        }
      }

      actions.push({ type: 'UPDATE_DOMAIN_LIST', domains: DOMAIN_LIST });
      if (_lastLifecycleAccountIds && _lastLifecycleAccountIds.length > 0) {
        actions.unshift({ type: 'UPDATE_ACCOUNTS', accountIds: _lastLifecycleAccountIds });
        _lastLifecycleAccountIds = null;
      }
      return actions;
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

let _localState = 'IDLE';
let _lastTransitionedAt = null; // last state change timestamp for temporal alignment in reconciliation

// ── Cadence tracking — updated on every CADENCE_TICK ────────────────────────
let _lastCadenceTickAt = null;
let _lastCadenceCycleCompletedAt = null; // updated by CADENCE_CYCLE_COMPLETED
let _lastCycleStartedAt = null; // set by CADENCE_TICK target function — cycle start timestamp
let _lastLifecycleAccountIds = null; // stored during LIFECYCLE_REFRESHED, emitted at cycle end

// ── Metrics buffer + cache — FSM-owned mutated state ──────────────────────
const _writeBuffer = [];       // [{ domain, status, latencyMs, accountId, metricId, ts }]
const _readCache = new Map();  // key → { data, cachedAt }
let _lastFlushAt = null;       // timestamp of last successful flush

// ── Canonical domain list — governance-controlled polling targets ─────────────
// All domains the sync substrate is permitted to poll. This list is the
// single source of truth — sync-substrate must NOT maintain its own hardcoded list.
const DOMAIN_LIST = [
  'comments',
  'messages',
  'ugc',
  'insights',
  'media',
  'publish:media',
  'publish:ugc',
  'publish:messaging',
];

// ── Default fail-open sanity check (universal gate pattern) ─────────────
// Same pattern as engagement-fsm. The ctx.sanityCheck is the
// universal gate; the FSM calls it during emission. For tests /
// non-CK dispatch, the default is always-allowed.
const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Dispatch
//
// Domain FSMs emit state transitions through the observability plane.
// Lineage authority is held by the lineage worker (Phase 2).
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

  // Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'scheduling',
        entity: 'fsm',
        entityId: 'scheduling-fsm',
        previousState: from,
        nextState: target,
        authority: 'scheduling-fsm',
        raw: { intent: event.type, accountIds: event.accountIds || null },
      });
    }
  } catch (_) {}

  const actions = (txn.buildActions ? await txn.buildActions(event, ctx) : []);

  // ── Track cadence tick timestamp for reconciliation engine ──────────────
  if (event.type === 'CADENCE_TICK') {
    _lastCadenceTickAt = Date.now();
  }

  console.log(`[scheduling-fsm] ${from} → ${target}  (${event.type})`);

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

/**
 * Returns the timestamp of the last CADENCE_TICK processed by this FSM.
 * Used by the reconciliation engine for cadence gap detection.
 * @returns {number|null} — Date.now() timestamp or null if never ticked
 */
function getLastCadenceTick() {
  return _lastCadenceTickAt;
}

function getLastCadenceCycleCompleted() {
  return _lastCadenceCycleCompletedAt;
}

/**
 * Returns the canonical domain list. Used by the sync substrate
 * to receive domain configuration via UPDATE_DOMAIN_LIST action.
 * @returns {string[]}
 */
function getDomainList() {
  return [...DOMAIN_LIST];
}

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

module.exports = {
  setGovernance,
  getGovernance,
  name: 'scheduling',
  dispatch,
  init,
  getState,
  exportState,
  getHealth,
  getLastCadenceTick,
  getLastCadenceCycleCompleted,
  getLastTransitionedAt,
  getDomainList,
};
