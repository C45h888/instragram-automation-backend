// control-plane/telemetry-workers/transition-writers/index.js
// Transition Writers: unified export for 5 bounded domain writers.
//
// Each writer is a dumb mechanical append pipe — it receives FSM-coordinated
// SEMANTIC_PROJECTION_TRANSITION entries (raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION')
// and writes them to the canonical ledger. It does not interpret, serialize, or
// write to namespace projection state.
//
// Architecture:
//   Phase 1: projection workers emit PROJECTION_INTENT to observability
//            → written to domain-bounded partition: lineage:transitionLog:domain:{namespace}
//   Phase 2: FSM reactive coordination → _emitTransition() → SEMANTIC_PROJECTION_TRANSITION
//            → written to global + domain-bounded transition log
//            → onWrite fires → 5 transition-writers each receive the event
//              → filter: raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION'? YES
//              → filter: domain === <this-writer's-namespace>? MATCH
//                → recordWorkerEntry() → lineage:ledger:entries
//                → CK.dispatch(PROJECTION_PERSISTED)
//   Phase 3: CK async validates → PROJECTION_ACCEPTED → namespace-projection-interpreter
//
// Health reporting:
//   getHealth() returns aggregate health across all 5 writers.
//   A single degraded writer triggers DEGRADED aggregate status.
//   All writers must be healthy for OK status.
//
// Deterministic recycle:
//   Timer-based recycling has been removed. Workers run until degraded.
//   When degraded, the health check triggers a deterministic 5-second grace period
//   then recycle. No timer-based kill/restart — state-based only.

const runtimeWriter = require('./runtime-transition-writer');
const integrityWriter = require('./integrity-transition-writer');
const authorityWriter = require('./authority-transition-writer');
const healthWriter = require('./health-transition-writer');
const systemicWriter = require('./systemic-transition-writer');

const writers = {
  runtime: runtimeWriter,
  integrity: integrityWriter,
  authority: authorityWriter,
  health: healthWriter,
  systemic: systemicWriter,
};

/** All domain namespaces this module owns */
const NAMESPACES = Object.freeze(Object.keys(writers));

// ── Deterministic recycle state ─────────────────────────────────────────────
let _recycleScheduled = false;

// ── Deterministic recycle ─────────────────────────────────────────────────────
// Replaces timer-based kill/restart cycles.
// When health transitions to DEGRADED or FAILED, schedule a restart after
// a grace period. This gives transient failures time to clear before
// a full recycle, and prevents thrashing if failures are continuous.
async function _deterministicRecycle() {
  const health = getHealth();
  console.log(
    `[transition-writers] Deterministic recycle triggered — ` +
    `writers: ${health.writers.map(w => `${w.namespace}:${w.ok ? 'ok' : w.degraded ? 'degraded' : 'failed'}`).join(', ')}`
  );

  // Stop all writers
  stopAll();

  // Brief pause for OS to release resources/connections
  await new Promise(r => setTimeout(r, 200));

  // Restart all writers
  startAll();

  _recycleScheduled = false;

  const newHealth = getHealth();
  console.log(
    `[transition-writers] Recycle complete — status: ${newHealth.status}, ` +
    `writes: ${newHealth.totalWrites}`
  );
}

/**
 * Start all 5 transition writers.
 * Writers subscribe to observability.onWrite() and filter for FSM-coordinated output.
 * Must be called AFTER observability.init() and BEFORE constitutional.startLoop().
 */
function startAll() {
  for (const writer of Object.values(writers)) {
    writer.start();
  }
  console.log('[transition-writers] All 5 writers started — event-driven, bounded by namespace');
}

/**
 * Stop all 5 transition writers gracefully.
 * Unsubscribes from observability.onWrite() hook.
 */
function stopAll() {
  for (const writer of Object.values(writers)) {
    writer.stop();
  }
  console.log('[transition-writers] All 5 writers stopped');
}

/**
 * Return health signals for all writers.
 * @returns {object} per-namespace health status keyed by namespace name
 */
function getAllHealth() {
  const result = {};
  for (const [name, writer] of Object.entries(writers)) {
    result[name] = writer.getHealth();
  }
  return result;
}

/**
 * Return aggregate health across all transition writers.
 * Also triggers deterministic recycle if writers are degraded.
 * This is the single status call used by CK, ingress-consistency substrate,
 * and the telemetry FSM to determine if the writer layer is healthy.
 *
 * Aggregate status derivation:
 *   OK       — all writers have ok:true
 *   DEGRADED — at least one writer degraded (some writes failing, not all)
 *   FAILED   — at least one writer has failed:true (all writes failing) OR all writers stopped
 *   STOPPED  — all writers have running:false
 *
 * @returns {object} aggregate health status
 */
function getHealth() {
  // ── Check and schedule recycle if needed ──────────────────────────────────
  const writerHealth = Object.values(writers).map(w => w.getHealth());

  const totalWrites = writerHealth.reduce((sum, h) => sum + (h.writeCount || 0), 0);
  const totalFailed = writerHealth.reduce((sum, h) => sum + (h.failedWrites || 0), 0);
  const totalCkFailures = writerHealth.reduce((sum, h) => sum + (h.ckDispatchFailures || 0), 0);
  const totalErrors = totalFailed + totalCkFailures;

  const allOk = writerHealth.every(h => h.ok);
  const anyDegraded = writerHealth.some(h => h.degraded);
  const anyFailed = writerHealth.some(h => h.failed);
  const anyStopped = writerHealth.every(h => !h.running);

  const currentStatus =
    anyStopped ? 'STOPPED' :
    anyFailed ? 'FAILED' :
    (anyDegraded || totalErrors > 0) ? 'DEGRADED' :
    allOk ? 'OK' : 'UNKNOWN';

  // ── Schedule deterministic recycle if degraded ──────────────────────────────
  if (currentStatus === 'DEGRADED' && !_recycleScheduled) {
    _recycleScheduled = true;
    setTimeout(() => {
      _deterministicRecycle().catch(err => {
        console.error('[transition-writers] Deterministic recycle failed:', err.message);
        _recycleScheduled = false;
      });
    }, 5000); // 5s grace period before recycle
  }

  // Collect error categories across all writers for alerting routing

  // Collect error categories across all writers for alerting routing
  const errorCategories = {};
  for (const h of writerHealth) {
    if (h.errorCounts) {
      for (const [cat, count] of Object.entries(h.errorCounts)) {
        if (count > 0) errorCategories[cat] = (errorCategories[cat] || 0) + count;
      }
    }
  }

  return {
    status: currentStatus, // OK | DEGRADED | FAILED | STOPPED | UNKNOWN
    totalWrites,
    totalFailed,
    totalCkFailures: totalCkFailures,
    totalErrors,
    writers: writerHealth,
    errorCategories: Object.keys(errorCategories).length > 0 ? errorCategories : null,
    // Worst-case error category across all writers (for alert routing priority)
    worstCategory: Object.entries(errorCategories)
      .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null,
    timestamp: Date.now(),
  };
}

module.exports = {
  writers,
  NAMESPACES,
  startAll,
  stopAll,
  getAllHealth,
  getHealth,
};