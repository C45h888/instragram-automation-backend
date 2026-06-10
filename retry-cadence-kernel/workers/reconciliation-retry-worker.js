// retry-cadence-kernel/workers/reconciliation-retry-worker.js
// Reconciliation Retry Worker: bounded executor for reconciliation cycles.
//
// Delegates to CK.triggerReconciliationRetry() which re-runs the full
// reconciliation cycle (lineage snapshot + engine comparison + checkpoint
// gate). CK sets _reconRetryFlight to prevent recursive retry emission.
//
// CONSTITUTIONAL CONTRACT:
//   - One bounded execution call. Delegates to CK to re-run the cycle.
//     Emits WORKER_OUTCOME_REPORTED with outcome.
//   - Does NOT classify, schedule, decide, or mutate FSM state.
//   - The retry cadence (scheduling, backoff, classification) is
//     engagement-fsm's domain.
//
// Receives: (domain, accountId, intentId, params, retryCount,
//            maxRetries, governance) from engagement-fsm._executeRetry.
// governance is the CK module ref.

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  // Dual-signature shim: CK gate passes single object, fallback passes positional args
  if (typeof domain === 'object' && domain !== null) {
    ({ domain, accountId, intentId, params, retryCount, maxRetries, governance } = domain);
  }

  const startTime = Date.now();

  // Gate: governance must have triggerReconciliationRetry
  if (!governance || typeof governance.triggerReconciliationRetry !== 'function') {
    console.error('[reconciliation-retry-worker] governance ref missing triggerReconciliationRetry');
    return;
  }

  try {
    // Delegate to CK — it runs the full reconciliation cycle.
    // _reconRetryFlight flag prevents recursive WORKER_OUTCOME_REPORTED emission.
    governance.triggerReconciliationRetry();

    // The cycle runs synchronously inside CK's dispatch loop.
    // On success, CK dispatches RECONCILIATION_RESULTS_RECEIVED +
    // RECONCILIATION_CYCLE_COMPLETE which transitions the FSM normally.
    // Report completion.
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'completed',
      result: { operation: 'reconciliation-cycle' },
      error: null,
      errorShape: null,
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  } catch (err) {
    // CK's triggerReconciliationRetry should not throw (it handles
    // failures internally), but defensive catch.
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'WORKER_OUTCOME_REPORTED',
      accountId, intentId, domain,
      status: 'failed',
      result: null,
      error: err.message,
      errorShape: {
        category: 'transient',
        code: null,
        retryable: true,
        retryAfterSeconds: null,
      },
      latencyMs: Date.now() - startTime,
      retryCount,
    });
  }
}

module.exports = { execute };
