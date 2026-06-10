// retry-cadence-kernel/substrates/retry-execution-substrate.js
// Retry Execution Substrate — bounded recovery logic for RETRY_OPERATION.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: deciding which worker to dispatch (connection, backoff, timeout),
//         validating the action shape, applying backoff from the substrate's
//         analysis, and emitting RESULT events through governance.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM as authority vector),
//                 state transition logic (FSM).
//
// Workers beneath this substrate:
//   connection-recovery-worker — re-runs the failed Supabase operation
//   backoff-enforcement-worker — delay execution, jitter, deferred queue
//   timeout-recovery-worker    — decomposes slow operations for architectural timeouts
//
// Flow:
//   FSM → RETRY_OPERATION_AUTHORIZED → retry-execution-substrate.execute()
//     → substrate decides: connection worker + backoff timer
//     → backoff worker applies delay
//     → connection worker re-dispatches to Supabase
//     → emits RETRY_EXECUTION_COMPLETE or RETRY_EXECUTION_FAILED

const policy = require('../policy');
const connectionRecoveryWorker = require('../workers/connection-recovery-worker');
const backoffEnforcementWorker = require('../workers/backoff-enforcement-worker');
const timeoutRecoveryWorker = require('../workers/timeout-recovery-worker');

/**
 * Execute a retry operation. Called by the FSM after RETRY_OPERATION_AUTHORIZED.
 *
 * @param {object} event — the RETRY_OPERATION_AUTHORIZED action
 * @param {object} governance — CK reference
 * @returns {Promise<object>} — { success, error, workerName, durationMs }
 */
async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, table, backoff, idempotencyKey, analysis } = event;

  if (!analysis) {
    return { success: false, error: 'analysis required', durationMs: Date.now() - startTime };
  }
  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  // ── Decide worker path based on category ────────────────────────────
  const category = analysis.category;
  const isTimeout = category === 'TIMEOUT' && analysis.timeout?.architecturalPressure;

  if (isTimeout) {
    // Architectural timeout → timeout-recovery-worker
    const result = await timeoutRecoveryWorker.execute({
      domain, accountId, intentId, table, analysis, backoff,
    }, governance);

    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_EXECUTION_COMPLETE',
      domain, accountId, intentId, table,
      route: 'timeout_recovery',
      success: result.success,
      error: result.error,
      workerName: 'timeout-recovery-worker',
      durationMs: Date.now() - startTime,
    });

    return { ...result, workerName: 'timeout-recovery-worker', durationMs: Date.now() - startTime };
  }

  // ── Standard retry: backoff → connection ────────────────────────────
  // 1. Apply backoff. The substrate owns the delay; this worker applies it.
  const backoffResult = await backoffEnforcementWorker.execute({
    domain, accountId, intentId, backoff,
  }, governance);

  if (!backoffResult.success) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_EXECUTION_FAILED',
      domain, accountId, intentId, table,
      route: 'backoff_blocked',
      success: false,
      error: backoffResult.error,
      workerName: 'backoff-enforcement-worker',
      durationMs: Date.now() - startTime,
    });
    return { success: false, error: backoffResult.error, workerName: 'backoff-enforcement-worker', durationMs: Date.now() - startTime };
  }

  // 2. Apply policy cap on the backoff delay
  const cappedDelayMs = policy.capDelay(
    backoffResult.appliedDelayMs || backoff?.computedMs || 0,
    domain
  );
  const actualDelayMs = Math.max(0, cappedDelayMs);

  // 3. Wait the delay (backoff already applied jitter in the worker)
  if (actualDelayMs > 0) {
    await new Promise(r => setTimeout(r, actualDelayMs));
  }

  // 4. Execute the connection recovery (re-run the Supabase operation)
  const connectionResult = await connectionRecoveryWorker.execute({
    domain, accountId, intentId, table, rows: event.rows || [], idempotencyKey, analysis, backoff,
  }, governance);

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: connectionResult.success ? 'RETRY_EXECUTION_COMPLETE' : 'RETRY_EXECUTION_FAILED',
    domain, accountId, intentId, table,
    route: 'connection_recovery',
    success: connectionResult.success,
    error: connectionResult.error,
    workerName: 'connection-recovery-worker',
    idempotencyKey,
    delayMs: actualDelayMs,
    durationMs: Date.now() - startTime,
  });

  return {
    ...connectionResult,
    workerName: 'connection-recovery-worker',
    delayMs: actualDelayMs,
    durationMs: Date.now() - startTime,
  };
}

module.exports = { execute };
