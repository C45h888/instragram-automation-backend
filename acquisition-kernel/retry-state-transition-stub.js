// acquisition-kernel/retry-state-transition-stub.js
// Phase 1 (base) — Retry state transition stub for the acquisition kernel.
//
// CONSTITUTIONAL CONTRACT (Phase 1 base — 2026-06-10):
//   - Pass-through adapter. NO state. NO retry counting.
//   - Routes acquisition-kernel's local events to the canonical
//     retry-cadence-kernel signals (RETRY_REQUESTED /
//     RETRY_EXHAUSTED / RETRY_IN_PROGRESS).
//   - The canonical retry state (count, maxRetries, lastError,
//     policy) lives in retry-cadence-kernel. The acquisition
//     kernel is a CLIENT, not a co-owner.
//
// Why this exists:
//   Pre-purge, acquisition-kernel/orchestrator.js forwarded
//   EXECUTE_ACQUISITION to RETRY_REQUESTED directly. That was
//   a constitutional bypass — the orchestrator became the
//   decision point for retry, bypassing the retry-cadence FSM.
//   This stub replaces the direct forwarder with an adapter
//   that preserves the kernel boundary: the orchestrator emits
//   a local action, the stub translates it to the canonical
//   RETRY_REQUESTED, the retry-cadence-kernel owns the decision.
//
// Does NOT own: retry state, retry policy, retry scheduling,
//               circuit-breaker state, classification.
// Does own: the mapping from acquisition-kernel's local event
//           vocabulary to the canonical retry-cadence signals.
//
// API:
//   requestRetry({ accountId, intentId, domain, params, error, errorShape, ... })
//     → emits RETRY_REQUESTED with the canonical envelope
//   notifyExhausted({ accountId, intentId, error, ... })
//     → emits RETRY_EXHAUSTED
//   notifyInProgress({ accountId, intentId, retryCount, delayMs, ... })
//     → emits RETRY_IN_PROGRESS
//   forwardExecuteAcquisition({ accountId, intentId, domain, params })
//     → convenience: emits RETRY_REQUESTED for the EXECUTE_ACQUISITION
//       action type, preserving the prior shape so the FSM can
//       consume it identically.

let _governance = null;

function setGovernance(gov) {
  _governance = gov;
}

function _emit(event) {
  if (!_governance) {
    console.warn('[acquisition:retry-stub] No governance reference; cannot emit', event.type);
    return;
  }
  (_governance.dispatchGlobal || _governance.dispatch)(event);
}

/**
 * Forward a retry request to the retry-cadence-kernel.
 * Canonical entry point for acquisition-side retry requests.
 *
 * @param {object} params — { accountId, intentId, domain, params, error,
 *                            errorShape, errorCategory, retryAfterMs }
 */
function requestRetry(params) {
  const { accountId, intentId, domain, error, errorShape, errorCategory, retryAfterMs } = params || {};
  _emit({
    type: 'RETRY_REQUESTED',
    accountId,
    domain: domain || 'acquisition',
    intentId: intentId || null,
    params: params?.params || {},
    error: error || null,
    errorShape: errorShape || null,
    errorCategory: errorCategory || 'transient',
    retryAfterMs: retryAfterMs ?? null,
  });
}

/**
 * Convenience: forward an EXECUTE_ACQUISITION action as a
 * RETRY_REQUESTED. Preserves the prior shape (domain, params,
 * intentId) so the retry-cadence FSM consumes it identically.
 *
 * @param {object} params — { accountId, intentId, domain, params }
 */
function forwardExecuteAcquisition(params) {
  const { accountId, intentId, domain, params: actionParams } = params || {};
  _emit({
    type: 'RETRY_REQUESTED',
    accountId,
    domain: domain || 'acquisition',
    intentId: intentId || null,
    params: actionParams || {},
  });
}

/**
 * Forward a retry-exhausted signal. Emitted by the acquisition
 * FSM on terminal failure (e.g. parsing_failed_retry_exhausted).
 *
 * @param {object} params — { accountId, intentId, error, ... }
 */
function notifyExhausted(params) {
  _emit({
    type: 'RETRY_EXHAUSTED',
    ...(params || {}),
    domain: params?.domain || 'acquisition',
  });
}

/**
 * Forward a retry-in-progress signal. The acquisition FSM
 * holds its state (PARSING, ACQUIRING) while the retry chain
 * is in flight.
 *
 * @param {object} params — { accountId, intentId, retryCount, delayMs, ... }
 */
function notifyInProgress(params) {
  _emit({
    type: 'RETRY_IN_PROGRESS',
    ...(params || {}),
    domain: params?.domain || 'acquisition',
  });
}

module.exports = {
  setGovernance,
  requestRetry,
  forwardExecuteAcquisition,
  notifyExhausted,
  notifyInProgress,
};
