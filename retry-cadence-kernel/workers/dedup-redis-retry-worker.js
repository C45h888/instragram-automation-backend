// retry-cadence-kernel/workers/dedup-redis-retry-worker.js
// Dedup-Redis Retry Worker: bounded executor for Redis dedup operations.
//
// Imports dedup-kernel/substrates/dedup directly — no intermediate
// retry substrate. The worker is the bounded I/O layer; engagement-fsm
// governs scheduling, classification-worker classifies errors, policy.js
// holds retry constants.
//
// CONSTITUTIONAL CONTRACT:
//   - One bounded I/O call per execute(). Runs the dedup operation
//     (check-dedup, mark-in-flight, clear-tick) via the canonical
//     dedup substrate. Emits WORKER_OUTCOME_REPORTED. Done.
//   - Does NOT classify, schedule, decide, or mutate FSM state.
//
// Receives: (domain, accountId, intentId, params, retryCount,
//            maxRetries, governance) from engagement-fsm._executeRetry.
// params.operation: 'check-dedup' | 'mark-in-flight' | 'clear-tick'

const dedupSubstrate = require('../../dedup-kernel/substrates/dedup');

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  // Dual-signature shim: CK gate passes { domain, accountId, ... } as
  // a single object; fallback path passes positional args. Normalise
  // so the rest of the function receives destructured locals.
  if (typeof domain === 'object' && domain !== null) {
    ({ domain, accountId, intentId, params, retryCount, maxRetries, governance } = domain);
  }

  const startTime = Date.now();
  const { operation, actionType, resourceId } = params;
  let result;

  try {
    switch (operation) {
      case 'check-dedup':
        result = await dedupSubstrate.isInFlight(accountId, actionType, resourceId, intentId);
        break;
      case 'mark-in-flight':
        await dedupSubstrate.markInFlight(accountId, actionType, resourceId, { intentId });
        result = { marked: true };
        break;
      case 'clear-tick':
        dedupSubstrate.clearTick();
        result = { cleared: true };
        break;
      default:
        result = { _error: true, error: `unknown_operation:${operation}`, category: 'permanent' };
    }

    // Handle substrate-level permanent error
    if (result && result._error) {
      (governance.dispatchGlobal || governance.dispatch)({
        type: 'WORKER_OUTCOME_REPORTED',
        accountId, intentId, domain,
        status: 'failed',
        result: null,
        error: result.error,
        errorShape: { category: result.category, code: null, retryable: false, retryAfterSeconds: null },
        latencyMs: Date.now() - startTime,
        retryCount,
      });
      return;
    }

    // Success
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'completed',
      result: { ...result, operation },
      error: null, errorShape: null,
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  } catch (err) {
    // Redis/network threw
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      result: null,
      error: err.message,
      errorShape: {
        category: _classify(err),
        code: err.code || null,
        retryable: _classify(err) === 'transient',
        retryAfterSeconds: null,
      },
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  }
}

function _classify(err) {
  const m = (err.message || '').toLowerCase();
  if (/etimedout|econnrefused|timeout|connect|econnreset|readonly/i.test(m)) return 'transient';
  if (/invalid/i.test(m)) return 'permanent';
  return 'transient';
}

module.exports = { execute };
