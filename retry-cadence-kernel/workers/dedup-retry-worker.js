// retry-cadence-kernel/workers/dedup-retry-worker.js
// Dedup Retry Worker: operationally bounded, semantically blind.
//
// CONSTITUTIONAL CONTRACT:
//   - Bounded single I/O call. Re-runs the failed dedup operation
//     via the dedup substrate. Emits WORKER_OUTCOME_REPORTED
//     with raw outcome. That is all.
//   - Does NOT classify errors. The classification-worker does that.
//   - Does NOT decide retry vs skip vs break.
//   - Does NOT schedule retries. engagement-fsm owns scheduling.
//   - Does NOT call other workers.
//   - Does NOT mutate dedup FSM state.
//   - The retryCount parameter is informational — the FSM owns
//     the canonical count.
//
// Receives: { domain, accountId, intentId, params, retryCount,
//             maxRetries, governance } from engagement-fsm via
//             _executeRetry.
//
// params.operation determines which dedup op to re-run:
//   'check-dedup'     → substrate.isInFlight(accountId, actionType, resourceId, intentId)
//   'mark-in-flight'  → substrate.markInFlight(accountId, actionType, resourceId, { intentId })
//   'clear-tick'      → substrate.clearTick()
//
// Authority chain: CK → engagement-fsm → _executeRetry →
//   this worker → dedup substrate → governance.dispatch(WORKER_OUTCOME_REPORTED)

const dedupSubstrate = require('../../dedup-kernel/substrates/dedup');

/**
 * Execute a single bounded retry attempt for a dedup operation.
 *
 * @param {string} domain — 'dedup'
 * @param {string} accountId
 * @param {string} intentId
 * @param {object} params — { operation, actionType, resourceId, accountId }
 * @param {number} retryCount — informational
 * @param {number} maxRetries — informational
 * @param {object} governance — CK module (dispatch)
 */
async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  const startTime = Date.now();
  const { operation, actionType, resourceId } = params;
  let result;

  try {
    switch (operation) {
      case 'check-dedup': {
        result = await dedupSubstrate.isInFlight(accountId, actionType, resourceId, intentId);
        break;
      }
      case 'mark-in-flight': {
        await dedupSubstrate.markInFlight(accountId, actionType, resourceId, { intentId });
        result = { marked: true };
        break;
      }
      case 'clear-tick': {
        dedupSubstrate.clearTick();
        result = { cleared: true };
        break;
      }
      default: {
        result = {
          success: false,
          error: `unknown_dedup_operation: ${operation}`,
          code: null,
          error_category: 'permanent',
        };
        break;
      }
    }

    // If result already has success=false (from default case), skip
    if (result && result.success === false) {
      // Already a failure — fall through to errorShape
    } else {
      // Success path
      const latencyMs = Date.now() - startTime;
      governance.dispatch({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId, intentId, domain,
        status: 'completed',
        result: { ...result, operation },
        error: null,
        errorShape: null,
        latencyMs,
        retryCount,
      });
      return;
    }
  } catch (err) {
    // Transport/substrate threw — wrap as transient error
    result = {
      success: false,
      error: err.message,
      code: err.code || null,
      error_category: _classifyErrorCategory(err),
    };
  }

  // Failure path — emit raw outcome upward
  const latencyMs = Date.now() - startTime;
  governance.dispatch({
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status: 'failed',
    result: null,
    error: result.error || null,
    errorShape: {
      category: result.error_category || 'unknown',
      code: result.code || null,
      retryable: result.error_category !== 'permanent',
      retryAfterSeconds: null,
    },
    latencyMs,
    retryCount,
  });
}

/**
 * Classify a thrown error into an error category for the errorShape.
 * Dedup-specific: Redis errors are transient; invalid args are permanent.
 */
function _classifyErrorCategory(err) {
  const msg = (err.message || '').toLowerCase();
  // Redis connectivity/timeout → transient
  if (msg.includes('etimedout') || msg.includes('econnrefused') ||
      msg.includes('timeout') || msg.includes('connect') ||
      msg.includes('econnreset') || msg.includes('readonly')) {
    return 'transient';
  }
  // Invalid argument / configuration → permanent
  if (msg.includes('invalid') || msg.includes('unknown_dedup_operation')) {
    return 'permanent';
  }
  return 'transient'; // default: transient — safe to retry once
}

module.exports = { execute };
