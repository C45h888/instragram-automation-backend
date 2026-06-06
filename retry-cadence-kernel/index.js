// retry-cadence-kernel/index.js
// Retry-cadence kernel entry point: paired-dispatch state store +
// per-substrate policy lookup.
//
// CONSTITUTIONAL CONTRACT (Step 4 of authority centralisation):
//   - Owns: per-intent retry state (count, maxRetries, lastError,
//            params). Per-substrate policy lookup. The pair of
//            (retryWorker, classificationWorker) bound to each
//            intent for paired-dispatch (refinement 1).
//   - Does NOT own: scheduling (engagement-fsm owns timers).
//   - Does NOT own: classification (classification-worker inside
//            engagement-fsm call).
//   - Does NOT own: state mutation (engagement-fsm owns _executionRetries).
//
// API:
//   dispatch(domain, accountId, intentId, params)
//     → looks up or creates the retry entry
//     → returns { allowed, count, maxRetries, retryWorker,
//                 classificationWorker, policy, lastError }
//     → does NOT schedule, does NOT set a timer
//   getRetryState(intentId)
//     → returns the entry for FSM inspection
//   clearRetry(intentId)
//     → deletes the entry (called on RETRY_EXHAUSTED / AUTH_SUCCESS)
//
// The FSM holds the canonical retry count via _executionRetries.
// This module holds the operational context (params, lastError,
// the paired workers) so the FSM can invoke them with full state.

const { getPolicy } = require('./policy');
const substrateRegistry = require('../acquisition-kernel/substrate-registry');

// ── Retry state (count, params, lastError, paired workers) ──────────────
// NOTE: timeoutId is intentionally NOT stored here. The FSM owns
// timers now (refinement 2 of Step 4). The FSM tracks timeoutIds
// in its own per-intent state. We track count + context.
const _retries = new Map(); // intentId → { domain, accountId, params,
                            //           count, maxRetries, lastError,
                            //           classificationWorker,
                            //           retryWorker, policy,
                            //           createdAt, updatedAt }

let _governance = null;

/**
 * Wire CK reference. Called by orchastrator at boot.
 */
function setGovernance(gov) {
  _governance = gov;
}

/**
 * Look up or create a retry entry. Returns the paired-dispatch context.
 * Does NOT schedule — the FSM owns scheduling.
 *
 * @param {string} domain
 * @param {string} accountId
 * @param {string} intentId
 * @param {object} params
 * @returns {{ allowed: boolean, count: number, maxRetries: number,
 *            retryWorker: object|null, classificationWorker: object|null,
 *            policy: object, lastError: object|null }}
 */
function dispatch(domain, accountId, intentId, params) {
  const policy = getPolicy(domain);
  const retryWorker = substrateRegistry.getRetryWorker(domain);
  const classificationWorker = substrateRegistry.getClassificationWorker(domain);

  if (!retryWorker || !classificationWorker) {
    console.error(`[retry-cadence] Missing worker pair for domain: ${domain}`);
    return {
      allowed: false, count: 0, maxRetries: 0,
      retryWorker: null, classificationWorker: null,
      policy, lastError: null,
    };
  }

  const existing = _retries.get(intentId);
  const now = Date.now();

  if (existing) {
    // Update existing entry — do not reset count. The FSM owns count
    // decisions; this module just holds the context.
    existing.updatedAt = now;
    existing.params = params;
    existing.retryWorker = retryWorker;
    existing.classificationWorker = classificationWorker;
    existing.policy = policy;
    return {
      allowed: existing.count < policy.maxRetries,
      count: existing.count,
      maxRetries: policy.maxRetries,
      retryWorker,
      classificationWorker,
      policy,
      lastError: existing.lastError || null,
    };
  }

  // New entry
  const entry = {
    domain,
    accountId,
    params,
    count: 0,
    maxRetries: policy.maxRetries,
    lastError: null,
    retryWorker,
    classificationWorker,
    policy,
    createdAt: now,
    updatedAt: now,
  };
  _retries.set(intentId, entry);

  return {
    allowed: true,
    count: 0,
    maxRetries: policy.maxRetries,
    retryWorker,
    classificationWorker,
    policy,
    lastError: null,
  };
}

/**
 * Update the lastError for an intent. Called by the FSM after a
 * worker reports WORKER_OUTCOME_REPORTED with a failure.
 *
 * @param {string} intentId
 * @param {object} errorShape
 */
function recordFailure(intentId, errorShape) {
  const entry = _retries.get(intentId);
  if (entry) {
    entry.lastError = errorShape;
    entry.updatedAt = Date.now();
  }
}

/**
 * Increment the retry count for an intent. Called by the FSM
 * when it has decided to schedule a retry. The count is the
 * canonical attempt counter.
 *
 * @param {string} intentId
 * @returns {number} new count
 */
function incrementCount(intentId) {
  const entry = _retries.get(intentId);
  if (!entry) return 0;
  entry.count += 1;
  entry.updatedAt = Date.now();
  return entry.count;
}

/**
 * Inspect the current retry state for an intent.
 *
 * @param {string} intentId
 * @returns {object|null}
 */
function getRetryState(intentId) {
  const entry = _retries.get(intentId);
  if (!entry) return null;
  // Return a snapshot — caller should not mutate
  return {
    domain: entry.domain,
    accountId: entry.accountId,
    params: entry.params,
    count: entry.count,
    maxRetries: entry.maxRetries,
    lastError: entry.lastError,
    policy: entry.policy,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
  };
}

/**
 * Clear a retry entry. Called by the FSM on terminal paths
 * (RETRY_EXHAUSTED, AUTH_SUCCESS, DISCONNECT_ACCOUNT).
 *
 * @param {string} intentId
 */
function clearRetry(intentId) {
  _retries.delete(intentId);
}

/**
 * Return all retry entries. For observability / reconciliation.
 */
function getAllRetries() {
  return new Map(_retries);
}

module.exports = {
  dispatch,
  recordFailure,
  incrementCount,
  getRetryState,
  clearRetry,
  getAllRetries,
  setGovernance,
};
