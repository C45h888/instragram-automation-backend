// retry-cadence-kernel/workers/resource-throttling-worker.js
// Resource Throttling Worker — bounded concurrency reduction.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: applying concurrency deltas, enforcing cooldown periods,
//         emitting throttle telemetry.
//
//   Does NOT own: throttle detection (throttle-substrate),
//                 classification (persistence-failure-substrate),
//                 persistent escalation (throttle-substrate).
//
// Called by: throttle-substrate.

/**
 * Apply a concurrency delta to the scheduling layer.
 *
 * The scheduling-kernel's concurrency counter is the canonical source.
 * This worker dispatches a CONCURRENCY_DELTA_APPLIED event that the
 * scheduler consumes. The worker does not directly mutate scheduler
 * state — it signals through governance.
 *
 * @param {object} params — { domain, accountId, intentId, delta, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, appliedDelta: number }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, delta, analysis } = params;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, appliedDelta: 0 };
  }

  const appliedDelta = delta || -1;

  // Signal the scheduling kernel to reduce concurrency
  (governance.dispatchGlobal || governance.dispatch)({
    type: 'CONCURRENCY_DELTA_APPLIED',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId: intentId || '*',
    delta: appliedDelta,
    source: 'resource-throttling-worker',
    reason: analysis?.resourceExhaustion?.signal || 'rate_limit_throttle',
  });

  return { success: true, appliedDelta };
}

module.exports = { execute };
