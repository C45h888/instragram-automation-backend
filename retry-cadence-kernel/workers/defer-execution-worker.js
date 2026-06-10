// retry-cadence-kernel/workers/defer-execution-worker.js
// Defer Execution Worker — bounded deferred queue and re-injection.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: deferred queue storage, delayed re-injection, queue drain.
//
//   Does NOT own: deferral decision (FSM), backoff timing (backoff-enforcement-worker),
//                 actual execution (connection-recovery-worker).
//
// Called by: deferral-substrate.

const _deferredQueue = []; // { domain, accountId, intentId, analysis, deferredAt, retryAfterMs }

/**
 * Enqueue a deferred operation.
 *
 * @param {object} params — { domain, accountId, intentId, analysis }
 * @param {object} governance — CK reference
 * @returns {{ success: boolean, queueSize: number }}
 */
async function enqueue(params, governance) {
  const { domain, accountId, intentId, analysis } = params;

  // Compute retryAfterMs from the analysis. Rate-limit has retryAfterMs.
  // Resource exhaustion falls back to the substrate's backoff.
  const retryAfterMs =
    analysis?.rateLimit?.retryAfterMs
    || analysis?.backoff?.computedMs
    || 30000;  // default 30s

  _deferredQueue.push({
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId: intentId || `deferred-${Date.now()}`,
    analysis,
    deferredAt: Date.now(),
    retryAfterMs,
  });

  return { success: true, queueSize: _deferredQueue.length };
}

/**
 * Drain the deferred queue — re-inject operations whose retryAfterMs
 * has elapsed as RETRY_REQUESTED events through governance.
 *
 * @param {object} governance — CK reference
 * @returns {Array} — the drained entries
 */
function drain(governance) {
  const now = Date.now();
  const ready = [];
  const stillWaiting = [];

  for (const entry of _deferredQueue) {
    if (now - entry.deferredAt >= entry.retryAfterMs) {
      ready.push(entry);
    } else {
      stillWaiting.push(entry);
    }
  }

  // Re-inject ready entries as RETRY_REQUESTED
  for (const entry of ready) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_REQUESTED',
      domain: entry.domain,
      accountId: entry.accountId,
      intentId: entry.intentId,
      analysis: entry.analysis,
      source: 'deferred-queue-drain',
      deferredAt: entry.deferredAt,
    });
  }

  // Update queue with only the still-waiting entries
  _deferredQueue.length = 0;
  _deferredQueue.push(...stillWaiting);

  return ready;
}

module.exports = { enqueue, drain };
