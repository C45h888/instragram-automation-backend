// retry-cadence-kernel/substrates/deferral-substrate.js
// Deferral Substrate — bounded deferred execution queue management.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: deferred queue management, delayed re-injection into the
//         scheduling pipeline, queue drain logic.
//
//   Does NOT own: decision to defer (FSM as authority vector),
//                 classification (persistence-failure-substrate),
//                 actual execution (retry-execution-substrate).
//
// Worker beneath: defer-execution-worker
//
// Flow:
//   FSM → DEFER_EXECUTION_AUTHORIZED → deferral-substrate.execute()
//     → defer-execution-worker enqueues the operation
//     → on DEFERRED_DRAIN (periodic or explicit trigger):
//       → worker re-injects as RETRY_REQUESTED
//     → emits DEFER_ENQUEUED or DEFER_DRAINED

const deferExecutionWorker = require('../workers/defer-execution-worker');

async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  const result = await deferExecutionWorker.enqueue({
    domain, accountId, intentId, analysis,
  }, governance);

  const durationMs = Date.now() - startTime;

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'DEFER_ENQUEUED',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    workerName: 'defer-execution-worker',
    queueSize: result.queueSize,
    durationMs,
  });

  return { ...result, durationMs };
}

/**
 * Drain the deferred queue — called by the scheduling kernel's periodic
 * tick or an explicit DEFERRED_DRAIN event from the FSM.
 *
 * Re-injects each deferred operation as RETRY_REQUESTED so the FSM
 * can re-evaluate and re-authorize.
 */
async function drain(governance) {
  const startTime = Date.now();
  const drained = deferExecutionWorker.drain(governance);

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'DEFERRED_DRAIN_COMPLETE',
    drainedCount: drained.length,
    durationMs: Date.now() - startTime,
  });

  return drained;
}

module.exports = { execute, drain };
