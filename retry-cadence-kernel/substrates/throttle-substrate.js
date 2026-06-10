// retry-cadence-kernel/substrates/throttle-substrate.js
// Throttle Substrate — bounded workload pacing and concurrency reduction.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: concurrency reduction, rate-limit pacing, persistent-throttle
//         detection and escalation to deferral.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 actual retry (retry-execution-substrate).
//
// Worker beneath: resource-throttling-worker
//
// Flow:
//   FSM → THROTTLE_WORKLOAD_AUTHORIZED → throttle-substrate.execute()
//     → resource-throttling-worker applies concurrency delta
//     → if persistent (3+ in window), escalates to DEFER_EXECUTION
//     → emits THROTTLE_APPLIED

const resourceThrottlingWorker = require('../workers/resource-throttling-worker');

// Track throttle events per domain+account for persistence detection
const _throttleWindow = new Map(); // key → { count, firstAt }

const THROTTLE_WINDOW_MS = 60000;   // 1 minute window
const THROTTLE_PERSISTENCE_THRESHOLD = 3;  // 3+in window → persistent

async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, recommendedConcurrencyDelta, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  const windowKey = `${domain || 'persist-telemetry'}|${accountId || '*'}`;
  const now = Date.now();
  let windowEntry = _throttleWindow.get(windowKey);

  if (!windowEntry || now - windowEntry.firstAt > THROTTLE_WINDOW_MS) {
    windowEntry = { count: 1, firstAt: now };
  } else {
    windowEntry.count++;
  }
  _throttleWindow.set(windowKey, windowEntry);

  // Dispatch to the worker
  const delta = recommendedConcurrencyDelta ?? -1;
  const result = await resourceThrottlingWorker.execute({
    domain, accountId, intentId, delta, analysis,
  }, governance);

  const durationMs = Date.now() - startTime;

  const isPersistent = windowEntry.count >= THROTTLE_PERSISTENCE_THRESHOLD;

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'THROTTLE_APPLIED',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    concurrencyDelta: delta,
    persistent: isPersistent,
    throttleCount: windowEntry.count,
    workerName: 'resource-throttling-worker',
    durationMs,
  });

  // If persistent throttle, escalate to deferral
  if (isPersistent) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'DEFER_EXECUTION_AUTHORIZED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      analysis,
      reason: `persistent throttle: ${windowEntry.count} events in ${THROTTLE_WINDOW_MS}ms`,
    });
  }

  return { ...result, persistent: isPersistent, durationMs };
}

module.exports = { execute };
