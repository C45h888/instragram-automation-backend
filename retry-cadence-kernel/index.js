// substrates/retry-cadence/index.js
// Retry-cadence substrate: entry point + retry state tracker.
//
// Owns: retry count tracking, dispatching to domain-specific workers,
//        cancelRetry for interrupted intents.
// Does NOT own: fetch execution, error classification, per-domain retry policy,
//               circuit breaker decisions — delegated to bounded workers.

const { getPolicy } = require('./policy');
const { getWorker } = require('./registry');

// ── Retry state ───────────────────────────────────────────────────────────────
const _retries = new Map(); // intentId → { domain, accountId, params, count, maxRetries, timeoutId }
let _governance = null;

/**
 * Wire CK reference. Called by orchastrator at boot.
 */
function setGovernance(gov) {
  _governance = gov;
}

/**
 * Dispatch a domain-specific retry. Called by engagement-fsm after
 * circuit breaker gate passes.
 *
 * @param {string} domain
 * @param {string} accountId
 * @param {string} intentId
 * @param {object} params — fetch parameters from the original intent
 * @returns {{ scheduled: boolean, retryCount: number, delayMs: number }}
 */
function dispatch(domain, accountId, intentId, params) {
  const worker = getWorker(domain);
  if (!worker) {
    console.error(`[retry-cadence] No worker for domain: ${domain}`);
    if (_governance) {
      _governance.dispatch({ type: 'RETRY_EXHAUSTED', accountId, domain, intentId, error: 'unknown_domain' });
    }
    return { scheduled: false, retryCount: 0, delayMs: 0 };
  }

  const policy = getPolicy(domain);
  const existing = _retries.get(intentId);
  const retryCount = (existing ? existing.count : 0) + 1;

  // Clear any previous pending timeout
  if (existing && existing.timeoutId) {
    clearTimeout(existing.timeoutId);
  }

  // Delegate to domain worker — worker handles fetch, classify, retry/recurse, exhaust
  const timeoutId = worker.schedule(
    domain, accountId, intentId, params, retryCount, policy.maxRetries, _governance
  );

  _retries.set(intentId, { domain, accountId, params, count: retryCount, maxRetries: policy.maxRetries, timeoutId });

  const delayMs = Math.min(policy.baseDelayMs * Math.pow(policy.backoffMultiplier, retryCount - 1), policy.maxDelayMs);
  console.log(`[retry-cadence] Retry #${retryCount}/${policy.maxRetries} for ${domain}/${accountId} in ${delayMs}ms`);

  return { scheduled: true, retryCount, delayMs };
}

/**
 * Cancel a pending retry for an intent (called on ACQUISITION_COMPLETE).
 */
function cancelRetry(intentId) {
  const entry = _retries.get(intentId);
  if (entry && entry.timeoutId) {
    clearTimeout(entry.timeoutId);
  }
  _retries.delete(intentId);
}

module.exports = { dispatch, cancelRetry, setGovernance };
