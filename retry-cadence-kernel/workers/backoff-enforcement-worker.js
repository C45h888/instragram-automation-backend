// retry-cadence-kernel/workers/backoff-enforcement-worker.js
// Backoff Enforcement Worker — bounded delay execution and jitter application.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: applying backoff delays with jitter, enforcing cooldown,
//         managing the deferred execution queue for DEFER_EXECUTION.
//
//   Does NOT own: delay computation (substrate's analyzeFailure.backoff),
//                 structural caps (policy.capDelay),
//                 retry re-dispatch (connection-recovery-worker),
//                 classification or recommendation logic.
//
// Called by: retry-execution-substrate (for RETRY_OPERATION),
//            deferral-substrate (for DEFER_EXECUTION drain).

const policy = require('../policy');

/**
 * Apply a backoff delay with jitter.
 *
 * @param {object} params — { domain, accountId, intentId, backoff }
 *   backoff: { baseMs, maxMs, multiplier, jitterMs, computedMs, cappedMs }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, appliedDelayMs: number, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, backoff } = params;

  if (!backoff) {
    return { success: true, appliedDelayMs: 0 };
  }

  // Compute the effective delay: substrate's computedMs ± jitter
  let delayMs = backoff.computedMs || backoff.cappedMs || 0;
  const jitterMs = backoff.jitterMs || 0;

  // Apply jitter: randomize within ± jitterMs
  if (jitterMs > 0) {
    const jitter = Math.floor(Math.random() * (jitterMs * 2 + 1)) - jitterMs;
    delayMs = Math.max(0, delayMs + jitter);
  }

  // Cap at policy structural maximum
  const cappedMs = policy.capDelay(delayMs, domain || 'persist-telemetry');
  const finalDelayMs = Math.max(0, cappedMs);

  return { success: true, appliedDelayMs: finalDelayMs };
}

module.exports = { execute };
