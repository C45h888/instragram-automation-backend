// telemetry-kernel/substrates/projection/transition-writers/base-transition-writer.js
// Base Transition Writer: mechanical append pipe for FSM-coordinated transitions.
// Migrated from control-plane/telemetry-workers/transition-writers/base-transition-writer.js
// with dependency paths adjusted for kernel location.
//
// Owns: reading from bounded domain partition of the transition log,
//        appending FSM output to canonical ledger, notifying CK for async validation.
//
// Does NOT own: semantic validation, namespace projection state, serialization,
//               intent interpretation, or any governance decision.
//
// Architectural identity:
//   This writer is a DUMB MECHANICAL PIPE. It receives FSM-coordinated output
//   (SEMANTIC_PROJECTION_TRANSITION with raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION')
//   and appends it to the canonical ledger. It does not interpret, validate, or derive.
//
// Trigger model: event-driven via observability.onWrite() — zero timers.
//   onWrite fires synchronously on every _transitionLog.push().
//   Writer filters: raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION' AND domain === <namespace>
//   → recordWorkerEntry() → CK.dispatch(PROJECTION_PERSISTED)
//   → returns immediately — never blocks on CK validation.
//
// Constitutional constraints:
//   - Does NOT write to namespace projection state (lineage:projection:{domain})
//   - Does NOT implement _serializeIntent — FSM's _serializeIntent is the authoritative serializer
//   - Does NOT filter by nextState === 'PROJECTION_INTENT' — only FSM output is consumed

// Transition writers are template-generated per namespace.
// Each writer receives the namespace it owns and the CK dispatch function.

// ═══════════════════════════════════════════════════════════════════════════════
// Error Classification Taxonomy
// ═══════════════════════════════════════════════════════════════════════════════
//
// Errors are classified into distinct categories that flow through the FSM chain:
//   - REDIS_UNAVAILABLE        → Redis connection refused / timeout / not ready
//   - LINEAGE_WRITE_FAILED     → Redis write to ledger failed (non-connection)
//   - CK_DISPATCH_FAILED       → CK.dispatch() threw during PROJECTION_PERSISTED
//   - SERIALIZATION_ERROR      → JSON.stringify error on entry
//   - UNKNOWN                  → unclassified
//
// Each category triggers a different escalation path through telemetry FSM
// and routes to a specific alert/response handler in engagement FSM.

const ERROR_CATEGORIES = Object.freeze({
  REDIS_UNAVAILABLE: 'REDIS_UNAVAILABLE',         // connection refused, timeout, ETIMEDOUT
  LINEAGE_WRITE_FAILED: 'LINEAGE_WRITE_FAILED',   // Redis write error (non-connection)
  CK_DISPATCH_FAILED: 'CK_DISPATCH_FAILED',       // CK.dispatch threw
  SERIALIZATION_ERROR: 'SERIALIZATION_ERROR',     // JSON/stringify error
  UNKNOWN: 'UNKNOWN',
});

const NAMESPACES = ['runtime', 'integrity', 'authority', 'health', 'systemic', 'persist-telemetry', 'reconciliation', 'scheduling', 'dedup', 'publishing', 'acquisition'];

/**
 * Classify an error into a named category for FSM routing.
 *
 * @param {Error|string} err — the error to classify
 * @returns {string} one of ERROR_CATEGORIES
 */
function _classifyError(err) {
  const msg = (err?.message ?? String(err)).toLowerCase();
  if (/ECONNREFUSED|ETIMEDOUT|ENOTFOUND|ENETUNREACH|timeout|connect|redis.*not.*ready/i.test(msg))
    return ERROR_CATEGORIES.REDIS_UNAVAILABLE;
  if (/lineage|ledger|write.*fail|rpush|lpush/i.test(msg))
    return ERROR_CATEGORIES.LINEAGE_WRITE_FAILED;
  if (/dispatch|projection_persisted|cannot.*read|cannot.*set/i.test(msg))
    return ERROR_CATEGORIES.CK_DISPATCH_FAILED;
  if (/json|stringify|circular|ioutil/i.test(msg))
    return ERROR_CATEGORIES.SERIALIZATION_ERROR;
  return ERROR_CATEGORIES.UNKNOWN;
}

/**
 * Creates a transition writer for a given namespace.
 * Each writer subscribes to observability.onWrite() and filters for:
 *   1. transition.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
 *   2. transition.domain === namespace
 *
 * @param {string} namespace — bounded domain: runtime | integrity | authority | health | systemic
 * @returns {{ start: Function, stop: Function, getHealth: Function, ERROR_CATEGORIES: Object }}
 */
function createTransitionWriter(namespace) {
  let _unsubscribe = null;
  let _startedAt = null;

  // ── Health state ──────────────────────────────────────────────────────────
  let _writeCount = 0;         // successful writes
  let _failedWrites = 0;       // failed writes (ledger errors, not CK errors)
  let _ckDispatchFailures = 0; // CK dispatch failures (separate from ledger failures)
  let _totalErrors = 0;        // total errors across all categories
  let _lastError = null;       // most recent error message
  let _lastErrorCategory = null; // most recent error category
  let _lastErrorAt = null;     // most recent error timestamp
  let _lastLedgerId = null;    // last successful ledgerId written

  // Per-error-category counters for fine-grained health reporting
  const _errorCounts = Object.fromEntries(
    Object.values(ERROR_CATEGORIES).map(c => [c, 0])
  );

  // Pending write tracker — stores the in-flight recordWorkerEntry promise
  // so callers can await write completion before asserting ledger state.
  let _pendingWrite = null;

  // ── Error tracking helper ────────────────────────────────────────────────
  function _recordError(err, category) {
    _lastError = err?.message ?? String(err);
    _lastErrorCategory = category;
    _lastErrorAt = Date.now();
  }

  function start() {
    // eslint-disable-next-line global-require
    const observability = require('../../../../control-plane/observability');
    // eslint-disable-next-line global-require
    const lineageLedger = require('../../../../control-plane/governance/lineage-ledger');
    // eslint-disable-next-line global-require
    const CK = require('../../../../control-plane/governance/constitutional-kernel');

    _unsubscribe = observability.onWrite(async (transition) => {
      // Gate 1: Structural filter — only FSM output (SEMANTIC_PROJECTION_TRANSITION) is consumed.
      // PROJECTION_INTENT entries (raw.entryType undefined or 'PROJECTION_INTENT') are skipped.
      // This structural check replaces the coordinatedBy flag — no split gate.
      if (transition.raw?.entryType !== 'SEMANTIC_PROJECTION_TRANSITION') return;

      // Gate 2: Domain filter — bounded to this writer's namespace
      if (transition.domain !== namespace) return;

      // Gate 3: Serialize entry before writing (catch stringify errors)
      let serializedEntry;
      try {
        serializedEntry = JSON.parse(JSON.stringify(transition));
      } catch (err) {
        _failedWrites++;
        _errorCounts[ERROR_CATEGORIES.SERIALIZATION_ERROR]++;
        _recordError(err, ERROR_CATEGORIES.SERIALIZATION_ERROR);
        console.error(`[${namespace}-transition-writer] Serialization error:`, err.message);
        return;
      }

      // ── Atomic write: ledger + CK dispatch ───────────────────────────────────
      // Ledger write is authoritative — if it succeeds, the entry IS persisted.
      // CK dispatch failure is secondary: mark FAILED rather than PENDING orphan.
      //
      // _pendingWrite is stored so callers (tests) can await the full chain:
      //   await transitionWriters.runtime.awaitPendingWrite()
      // This eliminates the fire-and-forget race where setImmediate fires before rpush completes.
      _pendingWrite = lineageLedger.recordWorkerEntry(serializedEntry).then((ledgerEntry) => {
        if (!ledgerEntry || !ledgerEntry.ledgerId) return;
        _lastLedgerId = ledgerEntry.ledgerId;
        _writeCount++;

        // Notify CK for async validation — fire-and-forget
        try {
          return Promise.resolve(CK.dispatch({
            type: 'PROJECTION_PERSISTED',
            ledgerId: _lastLedgerId,
            entry: serializedEntry,
          })).catch((err) => {
            const category = ERROR_CATEGORIES.CK_DISPATCH_FAILED;
            _ckDispatchFailures++;
            _errorCounts[category] = (_errorCounts[category] || 0) + 1;
            _totalErrors++;
            _recordError(err, category);
            console.error(`[${namespace}-transition-writer] CK dispatch error [${category}]:`, err.message);
          });
        } catch (err) {
          const category = ERROR_CATEGORIES.CK_DISPATCH_FAILED;
          _ckDispatchFailures++;
          _errorCounts[category] = (_errorCounts[category] || 0) + 1;
          _totalErrors++;
          _recordError(err, category);
          console.error(`[${namespace}-transition-writer] CK dispatch error [${category}]:`, err.message);
          return null;
        }
      }).catch(async (err) => {
        // Ledger write failed — entry was never persisted.
        // Track, classify, short-circuit. No CK dispatch.
        const category = _classifyError(err);
        _failedWrites++;
        _errorCounts[category] = (_errorCounts[category] || 0) + 1;
        _totalErrors++;
        _recordError(err, category);
        console.error(`[${namespace}-transition-writer] Ledger write error [${category}]:`, err.message);
      });
    });
  }

  /**
   * Await the in-flight write to complete before asserting ledger state.
   * Eliminates fire-and-forget timing races in tests.
   *
   * @param {number} [timeoutMs=5000] — max ms to wait
   * @returns {Promise<void>} resolves when write completes (success or failure), rejects on timeout
   */
  async function awaitPendingWrite(timeoutMs = 5000) {
    if (!_pendingWrite) return;
    await Promise.race([
      _pendingWrite,
      new Promise(r => setTimeout(r, timeoutMs)),
    ]);
 }

  function stop() {
    if (_unsubscribe) {
      try { _unsubscribe(); } catch (_) {}
      _unsubscribe = null;
    }
    console.log(
      `[${namespace}-transition-writer] Stopped — writes:${_writeCount} ` +
      `failed:${_failedWrites} ckDispatchFailures:${_ckDispatchFailures}`
    );
  }

  /**
   * Returns comprehensive health signal for this writer.
   * Used by: ingress-consistency substrate, telemetry FSM, CK health aggregation.
   *
   * @returns {object} health signal
   */
  function getHealth() {
    const uptimeMs = _startedAt ? Date.now() - _startedAt : 0;
    return {
      namespace,
      running: _unsubscribe !== null,
      // Write counts
      writeCount: _writeCount,
      failedWrites: _failedWrites,
      ckDispatchFailures: _ckDispatchFailures,
      // Error detail
      lastError: _lastError,
      lastErrorCategory: _lastErrorCategory,
      lastErrorAt: _lastErrorAt,
      lastLedgerId: _lastLedgerId,
      // Per-category breakdown
      errorCounts: { ..._errorCounts },
      // Derived status
      ok: _failedWrites === 0 && _ckDispatchFailures === 0,
      degraded: (_failedWrites > 0 || _ckDispatchFailures > 0) && _failedWrites < _writeCount,
      failed: _failedWrites > 0 && _failedWrites >= _writeCount,
      uptimeMs,
    };
  }

  return { start, stop, getHealth, awaitPendingWrite, ERROR_CATEGORIES };
}

module.exports = { createTransitionWriter, ERROR_CATEGORIES, NAMESPACES };
