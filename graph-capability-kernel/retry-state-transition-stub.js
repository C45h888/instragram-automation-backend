// graph-capability-kernel/retry-state-transition-stub.js
// Phase 1 (base) — Retry state transition stub for the graph-capability kernel.
//
// CONSTITUTIONAL CONTRACT (Phase 1 base — 2026-06-10):
//   - Pass-through adapter. NO state. NO retry counting.
//   - The graph-capability kernel does not currently route any
//     flow through the retry cadence (its FSM is autonomous —
//     credential resolution, scope caching, capability gating).
//   - This stub exists so all three kernels (graph-capability,
//     acquisition, publishing) are uniformly wired as clients
//     of the retry-cadence-kernel. The stub becomes the
//     canonical seam for any future retry-shaped events the
//     graph-capability kernel may need to emit (e.g. long-running
//     capability scans that need retry sovereignty).
//
// Does NOT own: retry state, retry policy, retry scheduling.
// Does own: the mapping from this kernel's local event vocabulary
//           to the canonical RETRY_REQUESTED / RETRY_EXHAUSTED /
//           RETRY_IN_PROGRESS signals consumed by the
//           retry-cadence-kernel.
//
// API:
//   requestRetry({ accountId, intentId, params, error, errorShape })
//     → emits RETRY_REQUESTED with the canonical envelope
//   notifyExhausted({ accountId, intentId, error, ... })
//     → emits RETRY_EXHAUSTED (caller is the FSM, this is the
//       terminal-failure forwarder)
//   notifyInProgress({ accountId, intentId, retryCount, delayMs })
//     → emits RETRY_IN_PROGRESS (the FSM holds state while
//       the retry chain is in flight)
//
// All three methods route through the governance reference
// (set via setGovernance). No direct substrate calls, no
// worker invocations, no timers.

let _governance = null;

function setGovernance(gov) {
  _governance = gov;
}

function _emit(event) {
  if (!_governance) {
    console.warn('[graph-capability:retry-stub] No governance reference; cannot emit', event.type);
    return;
  }
  (_governance.dispatchGlobal || _governance.dispatch)(event);
}

/**
 * Forward a retry request to the retry-cadence-kernel.
 * Used by graph-capability callers that need retry sovereignty
 * over a capability-related operation. No current callers —
 * the stub is wired for future use.
 *
 * @param {object} params — { accountId, intentId, params, error,
 *                            errorShape, errorCategory, retryAfterMs,
 *                            domain }
 */
function requestRetry(params) {
  const { accountId, intentId, domain, error, errorShape, errorCategory, retryAfterMs } = params || {};
  _emit({
    type: 'RETRY_REQUESTED',
    accountId,
    domain: domain || 'graph-capability',
    intentId: intentId || null,
    params: params?.params || {},
    error: error || null,
    errorShape: errorShape || null,
    errorCategory: errorCategory || 'transient',
    retryAfterMs: retryAfterMs ?? null,
  });
}

/**
 * Forward a retry-exhausted signal. Emitted by the kernel's FSM
 * when its local retry budget is exhausted and the operation
 * transitions to terminal failure.
 *
 * @param {object} params — { accountId, intentId, error, ... }
 */
function notifyExhausted(params) {
  _emit({
    type: 'RETRY_EXHAUSTED',
    ...(params || {}),
    domain: params?.domain || 'graph-capability',
  });
}

/**
 * Forward a retry-in-progress signal. Emitted when a retry has
 * been scheduled and the chain is in flight. The kernel's FSM
 * holds its state while the retry is in flight.
 *
 * @param {object} params — { accountId, intentId, retryCount, delayMs, ... }
 */
function notifyInProgress(params) {
  _emit({
    type: 'RETRY_IN_PROGRESS',
    ...(params || {}),
    domain: params?.domain || 'graph-capability',
  });
}

module.exports = {
  setGovernance,
  requestRetry,
  notifyExhausted,
  notifyInProgress,
};
